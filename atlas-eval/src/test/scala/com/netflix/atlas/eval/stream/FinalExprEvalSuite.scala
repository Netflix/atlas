/*
 * Copyright 2014-2017 Netflix, Inc.
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
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FinalExprEvalSuite extends FunSuite {

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  private def run(input: List[AnyRef]): List[MessageEnvelope] = {
    val future = Source(input)
      .via(new FinalExprEval)
      .flatMapConcat(s => s)
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf).toList
  }

  private def sources(vs: DataSource*): DataSources = {
    DataSources.of(vs: _*)
  }

  private def ds(id: String, uri: String): DataSource = {
    new DataSource(id, uri)
  }

  test("exception while parsing exprs") {
    val input = List(
      sources(ds("a", "http://atlas/graph?q=foo,:time"))
    )
    val output = run(input)
    assert(output.size === 1)
    output.foreach { env =>
      assert(env.getId === "a")

      val msg = "invalid expression [[http://atlas/graph?q=foo,:time]]: " +
      "IllegalArgumentException: No enum constant java.time.temporal.ChronoField.foo"
      assert(env.getMessage.toJson.contains(msg))
    }
  }

  test("division with no data should result in no data line") {
    val input = List(
      sources(ds("a", "http://atlas/graph?q=name,latency,:eq,:dist-avg")),
      TimeGroup(0L, List.empty[AggrDatapoint])
    )
    val output = run(input)
    assert(output.size === 1)
    output.foreach { env =>
      assert(env.getId === "a")
      val ts = env.getMessage.asInstanceOf[TimeSeriesMessage]
      assert(ts.label === "(NO DATA / NO DATA)")
    }
  }
}
