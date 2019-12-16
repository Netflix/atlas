/*
 * Copyright 2014-2019 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.JsonSupport
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class LwcToAggrDatapointSuite extends AnyFunSuite {

  implicit val system = ActorSystem(getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  private val step = 10000

  private val sumMetric = s"""{"id":"sum","expression":"name,cpu,:eq,:sum","frequency":$step}"""
  private val countMetric =
    s"""{"id":"count","expression":"name,cpu,:eq,:count","frequency":$step}"""

  private val input = List(
    s"""info: subscribe {"expression":"name,cpu,:eq,:avg","metrics":[$sumMetric,$countMetric]}""",
    """info: other {"type":"info","msg":"something"}""",
    """data: diagnostic {"type":"diagnostic","id":"sum","message":{"type":"error","message":"1"}}""",
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
    """data: metric {"timestamp":30000,"id":"sum","tags":{"name":"cpu"},"value":4.0}""",
    """data: diagnostic {"type":"diagnostic","id":"sum","message":{"type":"error","message":"2"}}"""
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
      .via(new LwcToAggrDatapoint(context))
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

  test("diagnostic messages are logged") {
    logMessages.clear()
    eval(input)
    assert(logMessages.size() === 2)
    List("1", "2").foreach { i =>
      logMessages.poll() match {
        case (_, msg: DiagnosticMessage) =>
          assert(msg.`type` === "error")
          assert(msg.message === i)
        case v =>
          fail(s"unexpected message: $v")
      }
    }
  }

  test("heartbeat messages are passed through") {
    val data = List(
      """data: heartbeat {"type":"heartbeat","timestamp":1234567890,"step":10}"""
    )
    val results = eval(data)
    assert(results.size === 1)

    val d = results.head
    assert(d.isHeartbeat)
    assert(d.timestamp === 1234567890)
    assert(d.step === 10)
  }

  test("invalid message") {
    val msg =
      """data: metric {"timestamp":20000,"id":"sum","tags":{"name":"cpu"},\u007F"value":3.0}"""
    val results = eval(List(msg))
    assert(results.size === 0)
  }
}
