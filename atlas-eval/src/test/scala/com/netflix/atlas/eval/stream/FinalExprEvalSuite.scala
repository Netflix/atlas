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
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FinalExprEvalSuite extends FunSuite {

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  private val interpreter = new ExprInterpreter(ConfigFactory.load())

  private def run(input: List[AnyRef]): List[MessageEnvelope] = {
    val future = Source(input)
      .via(new FinalExprEval(interpreter))
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

  private def group(i: Long, vs: AggrDatapoint*): TimeGroup[AggrDatapoint] = {
    val timestamp = i * 60000L
    TimeGroup(timestamp, vs.map(_.copy(timestamp = timestamp)).toList)
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

  private def checkValue(ts: TimeSeriesMessage, expected: Double): Unit = {
    ts.data match {
      case ArrayData(vs) =>
        assert(vs.length === 1)
        if (expected.isNaN)
          assert(vs(0).isNaN)
        else
          assert(vs(0) === expected)
      case v =>
        fail(s"unexpected data value: $v")
    }
  }

  test("aggregate with single datapoint per group") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, expr, "i-1", tags, 42.0)),
      group(2, AggrDatapoint(0, expr, "i-1", tags, 43.0)),
      group(3, AggrDatapoint(0, expr, "i-1", tags, 44.0)),
    )

    val output = run(input)

    val timeseries = output.filter(_.getMessage.isInstanceOf[TimeSeriesMessage])
    assert(timeseries.size === 4)
    val expectedTimeseries = List(Double.NaN, 42.0, 43.0, 44.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assert(env.getId === "a")
        val ts = env.getMessage.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }

    val diagnostics = output.filter(_.getMessage.isInstanceOf[DiagnosticMessage])
    assert(diagnostics.size === 3)
    val expectedDiagnostics = List(
      DiagnosticMessage.info(s"1970-01-01T00:01:00Z: 1 input datapoints for [$expr]"),
      DiagnosticMessage.info(s"1970-01-01T00:02:00Z: 1 input datapoints for [$expr]"),
      DiagnosticMessage.info(s"1970-01-01T00:03:00Z: 1 input datapoints for [$expr]")
    )
    diagnostics.zip(expectedDiagnostics).foreach {
      case (actual, expected) =>
        assert(actual.getMessage === expected)
    }
  }

  test("aggregate with multiple datapoints per group") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, expr, "i-1", tags, 42.0)),
      group(
        2,
        AggrDatapoint(0, expr, "i-1", tags, 43.0),
        AggrDatapoint(0, expr, "i-2", tags, 41.0),
        AggrDatapoint(0, expr, "i-3", tags, 45.0)
      ),
      group(
        3,
        AggrDatapoint(0, expr, "i-1", tags, 43.0),
        AggrDatapoint(0, expr, "i-2", tags, 44.0)
      ),
    )

    val output = run(input)

    val timeseries = output.filter(_.getMessage.isInstanceOf[TimeSeriesMessage])
    assert(timeseries.size === 4)
    val expectedTimeseries = List(Double.NaN, 42.0, 129.0, 87.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assert(env.getId === "a")
        val ts = env.getMessage.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }

    val diagnostics = output.filter(_.getMessage.isInstanceOf[DiagnosticMessage])
    assert(diagnostics.size === 3)
    val expectedDiagnostics = List(
      DiagnosticMessage.info(s"1970-01-01T00:01:00Z: 1 input datapoints for [$expr]"),
      DiagnosticMessage.info(s"1970-01-01T00:02:00Z: 3 input datapoints for [$expr]"),
      DiagnosticMessage.info(s"1970-01-01T00:03:00Z: 2 input datapoints for [$expr]")
    )
    diagnostics.zip(expectedDiagnostics).foreach {
      case (actual, expected) =>
        assert(actual.getMessage === expected)
    }

  }

  test("aggregate with multiple expressions") {
    val expr1 = DataExpr.Sum(Query.Equal("name", "rps"))
    val expr2 = DataExpr.Max(Query.Equal("name", "gc.pause"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(
        ds("a", s"http://atlas/graph?q=$expr1"),
        ds("b", s"http://atlas/graph?q=$expr2"),
      ),
      group(0, AggrDatapoint(0, expr1, "i-1", tags, 42.0)),
      group(
        1,
        AggrDatapoint(0, expr1, "i-1", tags, 43.0),
        AggrDatapoint(0, expr1, "i-2", tags, 41.0),
        AggrDatapoint(0, expr2, "i-1", tags, 45.0)
      ),
      group(
        2,
        AggrDatapoint(0, expr2, "i-1", tags, 43.0),
        AggrDatapoint(0, expr2, "i-3", tags, 49.0),
        AggrDatapoint(0, expr1, "i-2", tags, 44.0)
      ),
    )

    val output = run(input)

    val timeseries = output.filter(_.getMessage.isInstanceOf[TimeSeriesMessage])
    assert(timeseries.size === 3 + 3) // 3 for expr1, 3 for expr2

    val expectedTimeseries1 = scala.collection.mutable.Queue(42.0, 84.0, 44.0)
    val expectedTimeseries2 = scala.collection.mutable.Queue(Double.NaN, 45.0, 49.0)
    timeseries.foreach { env =>
      val actual = env.getMessage.asInstanceOf[TimeSeriesMessage]
      if (env.getId == "a")
        checkValue(actual, expectedTimeseries1.dequeue())
      else
        checkValue(actual, expectedTimeseries2.dequeue())
    }

    val diagnostics = output.filter(_.getMessage.isInstanceOf[DiagnosticMessage])
    assert(diagnostics.size === 3 + 2) // 3 for datasource a, 2 for datasource b

    val expectedDiagnostics1 = scala.collection.mutable.Queue(
      DiagnosticMessage.info(s"1970-01-01T00:00:00Z: 1 input datapoints for [$expr1]"),
      DiagnosticMessage.info(s"1970-01-01T00:01:00Z: 2 input datapoints for [$expr1]"),
      DiagnosticMessage.info(s"1970-01-01T00:02:00Z: 1 input datapoints for [$expr1]")
    )
    val expectedDiagnostics2 = scala.collection.mutable.Queue(
      DiagnosticMessage.info(s"1970-01-01T00:01:00Z: 1 input datapoints for [$expr2]"),
      DiagnosticMessage.info(s"1970-01-01T00:02:00Z: 2 input datapoints for [$expr2]")
    )
    diagnostics.foreach { env =>
      val actual = env.getMessage.asInstanceOf[DiagnosticMessage]
      if (env.getId == "a")
        assert(actual === expectedDiagnostics1.dequeue())
      else
        assert(actual === expectedDiagnostics2.dequeue())
    }
  }

  // https://github.com/Netflix/atlas/issues/693
  test("group by with binary operation") {
    val expr1 = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "rps")), List("node"))
    val expr2 = DataExpr.GroupBy(DataExpr.Count(Query.Equal("name", "rps")), List("node"))
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr1,$expr2,:div")),
      group(
        0, // Missing sum for i-2
        AggrDatapoint(0, expr1, "i-1", Map("node" -> "i-1"), 42.0),
        AggrDatapoint(0, expr2, "i-1", Map("node" -> "i-1"), 1.0),
        AggrDatapoint(0, expr2, "i-2", Map("node" -> "i-2"), 1.0),
      ),
      group(
        1,
        AggrDatapoint(0, expr1, "i-1", Map("node" -> "i-1"), 42.0),
        AggrDatapoint(0, expr1, "i-2", Map("node" -> "i-2"), 21.0),
        AggrDatapoint(0, expr2, "i-1", Map("node" -> "i-1"), 1.0),
        AggrDatapoint(0, expr2, "i-2", Map("node" -> "i-2"), 1.0),
      ),
      group(
        2, // Missing count for i-1
        AggrDatapoint(0, expr1, "i-1", Map("node" -> "i-1"), 42.0),
        AggrDatapoint(0, expr1, "i-2", Map("node" -> "i-2"), 21.0),
        AggrDatapoint(0, expr2, "i-2", Map("node" -> "i-2"), 1.0),
      ),
    )

    val output = run(input)

    val timeseries = output.filter(_.getMessage.isInstanceOf[TimeSeriesMessage])
    assert(timeseries.size === 4)
    timeseries.foreach { env =>
      val ts = env.getMessage.asInstanceOf[TimeSeriesMessage]
      if (ts.tags("node") == "i-1") {
        assert(ts.start < 120000)
        checkValue(ts, 42.0)
      } else {
        assert(ts.start > 0)
        checkValue(ts, 21.0)
      }
    }

    val diagnostics = output.filter(_.getMessage.isInstanceOf[DiagnosticMessage])
    assert(diagnostics.size === 6)

    val expectedDiagnostics = List(
      DiagnosticMessage.info(s"1970-01-01T00:00:00Z: 1 input datapoints for [$expr1]"),
      DiagnosticMessage.info(s"1970-01-01T00:00:00Z: 2 input datapoints for [$expr2]"),
      DiagnosticMessage.info(s"1970-01-01T00:01:00Z: 2 input datapoints for [$expr1]"),
      DiagnosticMessage.info(s"1970-01-01T00:01:00Z: 2 input datapoints for [$expr2]"),
      DiagnosticMessage.info(s"1970-01-01T00:02:00Z: 2 input datapoints for [$expr1]"),
      DiagnosticMessage.info(s"1970-01-01T00:02:00Z: 1 input datapoints for [$expr2]")
    )
    diagnostics.zip(expectedDiagnostics).foreach {
      case (actual, expected) =>
        assert(actual.getMessage === expected)
    }
  }

  // https://github.com/Netflix/atlas/issues/762
  test(":legend is honored") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr,legend+for+$$name,:legend")),
      group(0),
      group(1, AggrDatapoint(0, expr, "i-1", tags, 42.0)),
      group(2, AggrDatapoint(0, expr, "i-1", tags, 43.0)),
      group(3, AggrDatapoint(0, expr, "i-1", tags, 44.0))
    )

    val output = run(input)

    val timeseries = output.filter(_.getMessage.isInstanceOf[TimeSeriesMessage])
    assert(timeseries.size === 4)
    // tail to ignore initial no data entry
    timeseries.tail.foreach { env =>
      val ts = env.getMessage.asInstanceOf[TimeSeriesMessage]
      assert(ts.label === "legend for rps")
    }
  }
}
