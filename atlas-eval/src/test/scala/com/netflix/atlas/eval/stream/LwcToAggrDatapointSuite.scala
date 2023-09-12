/*
 * Copyright 2014-2023 Netflix, Inc.
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

import java.util.concurrent.ArrayBlockingQueue
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.JsonSupport
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LwcToAggrDatapointSuite extends FunSuite {

  implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val materializer = Materializer(system)

  private val step = 10000

  private val sumMetric = s"""{"id":"sum","expression":"name,cpu,:eq,:sum","frequency":$step}"""

  private val countMetric =
    s"""{"id":"count","expression":"name,cpu,:eq,:count","frequency":$step}"""

  private val input = List(
    s"""{"type":"subscription","expression":"name,cpu,:eq,:avg","metrics":[$sumMetric,$countMetric]}""",
    """{"type":"info","msg":"something"}""",
    """{"type":"diagnostic","id":"sum","message":{"type":"error","message":"1"}}""",
    """{"type":"datapoint","timestamp":0,"id":"sum","tags":{"name":"cpu"},"value":1.0}""",
    """{"type":"datapoint","timestamp":0,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """{"type":"datapoint","timestamp":10000,"id":"sum","tags":{"name":"cpu"},"value":2.0}""",
    """{"type":"datapoint","timestamp":10000,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """{"type":"info","msg":"something"}""",
    """{"type":"info","msg":"something"}""",
    """{"type":"info","msg":"something"}""",
    """{"type":"info","msg":"something"}""",
    """{"type":"datapoint","timestamp":20000,"id":"sum","tags":{"name":"cpu"},"value":3.0}""",
    """{"type":"datapoint","timestamp":20000,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """{"type":"datapoint","timestamp":30000,"id":"count","tags":{"name":"cpu"},"value":4.0}""",
    """{"type":"datapoint","timestamp":30000,"id":"sum","tags":{"name":"cpu"},"value":4.0}""",
    """{"type":"diagnostic","id":"sum","message":{"type":"error","message":"2"}}"""
  )

  private val logMessages = new ArrayBlockingQueue[(DataSource, JsonSupport)](10)

  private val context = new StreamContext(
    ConfigFactory.load(),
    null,
    materializer,
    dsLogger = (ds, msg) => logMessages.add(ds -> msg)
  )

  context.setDataSources(
    DataSources.of(
      new DataSource("abc", java.time.Duration.ofMinutes(1), "/api/v1/graph?q=name,cpu,:eq,:avg")
    )
  )

  private def eval(data: List[String]): List[AggrDatapoint] = {
    val future = Source(data)
      .map(ByteString.apply)
      .map(LwcMessages.parse)
      .map(msg => List(msg))
      .via(new LwcToAggrDatapoint(context))
      .flatMapConcat(Source.apply)
      .runWith(Sink.seq[AggrDatapoint])
    Await.result(future, Duration.Inf).toList
  }

  test("eval") {
    val results = eval(input)
    assertEquals(results.size, 8)

    val groups = results.groupBy(_.expr)
    assertEquals(groups.size, 2)

    val sumData = groups(DataExpr.Sum(Query.Equal("name", "cpu")))
    assertEquals(sumData.map(_.value).toSet, Set(1.0, 2.0, 3.0, 4.0))

    val countData = groups(DataExpr.Count(Query.Equal("name", "cpu")))
    assertEquals(countData.size, 4)
    assertEquals(countData.map(_.value).toSet, Set(4.0))
  }

  test("diagnostic messages are logged") {
    logMessages.clear()
    eval(input)
    assertEquals(logMessages.size(), 2)
    List("1", "2").foreach { i =>
      logMessages.poll() match {
        case (_, msg: DiagnosticMessage) =>
          assertEquals(msg.`type`, "error")
          assertEquals(msg.message, i)
        case v =>
          fail(s"unexpected message: $v")
      }
    }
  }

  test("heartbeat messages are passed through") {
    val data = List(
      """{"type":"heartbeat","timestamp":1234567890,"step":10}"""
    )
    val results = eval(data)
    assertEquals(results.size, 1)

    val d = results.head
    assert(d.isHeartbeat)
    assertEquals(d.timestamp, 1234567890L)
    assertEquals(d.step, 10L)
  }
}
