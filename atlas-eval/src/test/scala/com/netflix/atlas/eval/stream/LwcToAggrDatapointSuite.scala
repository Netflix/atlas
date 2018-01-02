/*
 * Copyright 2014-2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.eval.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LwcToAggrDatapointSuite extends FunSuite {

  implicit val system = ActorSystem(getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  private val step = 10000

  private val sumMetric = s"""{"id":"sum","expression":"name,cpu,:eq,:sum","frequency":$step}"""
  private val countMetric =
    s"""{"id":"count","expression":"name,cpu,:eq,:count","frequency":$step}"""

  private val input = List(
    s"""info: subscribe {"expression":"name,cpu,:eq,:avg","metrics":[$sumMetric,$countMetric]}""",
    """info: other {"type":"info","msg":"something"}""",
    """data: metric {"timestamp":0,"id":"sum","tags":{"name":"cpu"},"value":1.0}""",
    """data: metric {"timestamp":0,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """data: metric {"timestamp":10000,"id":"sum","tags":{"name":"cpu"},"value":2.0}""",
    """data: metric {"timestamp":10000,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """data: other {"type":"info","msg":"something"}""",
    """data: other {"type":"info","msg":"something"}""",
    """data: other {"type":"info","msg":"something"}""",
    """data: other {"type":"info","msg":"something"}""",
    """data: metric {"timestamp":20000,"id":"sum","tags":{"name":"cpu"},"value":3.0}""",
    """data: metric {"timestamp":20000,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """data: metric {"timestamp":30000,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """data: metric {"timestamp":30000,"id":"sum","tags":{"name":"cpu"},"value":4.0}"""
  )

  private def eval(data: List[String]): List[AggrDatapoint] = {
    val future = Source(data)
      .via(new LwcToAggrDatapoint)
      .runWith(Sink.seq[AggrDatapoint])
    Await.result(future, Duration.Inf).toList
  }

  test("eval") {
    val results = eval(input)
    assert(results.size === 8)

    val groups = results.groupBy(_.expr)
    assert(groups.size === 2)

    val sumData = groups(DataExpr.Sum(Query.Equal("name", "cpu")))
    assert(sumData.map(_.value).toSet === Set(1.0, 2.0, 3.0, 4.0))

    val countData = groups(DataExpr.Count(Query.Equal("name", "cpu")))
    assert(countData.size === 4)
    assert(countData.map(_.value).toSet === Set(4.0))
  }
}
