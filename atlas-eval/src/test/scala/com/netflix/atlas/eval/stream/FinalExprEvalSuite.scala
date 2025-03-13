/*
 * Copyright 2014-2025 Netflix, Inc.
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

import com.netflix.atlas.chart.model.LineStyle
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.MathExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.AggrValuesInfo
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.EvalDataRate
import com.netflix.atlas.eval.model.EvalDataSize
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.spectator.api.DefaultRegistry
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import java.awt.Color
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FinalExprEvalSuite extends FunSuite {

  private val step = 60000L

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private val interpreter = new ExprInterpreter(ConfigFactory.load())

  private val registry = new DefaultRegistry()

  private def run(input: List[AnyRef], noDataMsgs: Boolean = true): List[MessageEnvelope] = {
    val future = Source(input)
      .via(new FinalExprEval(interpreter, noDataMsgs))
      .flatMapConcat(s => s)
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf).toList
  }

  private def sources(vs: DataSource*): DataSources = {
    DataSources.of(vs*)
  }

  private def ds(id: String, uri: String, step: Long = 60000L): DataSource = {
    new DataSource(id, java.time.Duration.ofMillis(step), uri)
  }

  private def settings(maxInput: Int, maxIntermediate: Int): AggrDatapoint.AggregatorSettings = {
    AggrDatapoint.AggregatorSettings(maxInput, maxIntermediate, registry)
  }

  private def group(i: Long, vs: AggrDatapoint*): TimeGroup = {
    val timestamp = i * step
    val values = vs
      .map(_.copy(timestamp = timestamp))
      .groupBy(_.expr)
      .map(t => {
        val aggr =
          AggrDatapoint.aggregate(t._2.toList, settings(Integer.MAX_VALUE, Integer.MAX_VALUE)).get
        t._1 -> AggrValuesInfo(aggr.datapoints, aggr.numInputDatapoints)
      })
    TimeGroup(timestamp, step, values)
  }

  test("exception while parsing exprs") {
    val input = List(
      sources(ds("a", "http://atlas/graph?q=foo,:time"))
    )
    val output = run(input)
    assertEquals(output.size, 1)
    output.foreach { env =>
      assertEquals(env.id, "a")

      val msg = "invalid expression [[http://atlas/graph?q=foo,:time]]: " +
        "IllegalArgumentException: No enum constant java.time.temporal.ChronoField.foo"
      assert(env.message.toJson.contains(msg))
    }
  }

  test("ignore event exprs") {
    val input = List(
      sources(ds("a", "http://atlas/events?q=app,foo,:eq,(,message,),:table"))
    )
    val output = run(input)
    assert(output.isEmpty)
  }

  test("division with no data should result in no data line") {
    val input = List(
      sources(ds("a", "http://atlas/graph?q=name,latency,:eq,:dist-avg")),
      TimeGroup(0L, step, Map.empty)
    )
    val output = run(input)
    assertEquals(output.size, 1)

    val tsMsgs = output.filter(isTimeSeries)
    assertEquals(tsMsgs.size, 1)
    val (tsId, tsMsg) = tsMsgs.head.id -> tsMsgs.head.message.asInstanceOf[TimeSeriesMessage]
    assert(tsId == "a")
    assertEquals(tsMsg.label, "(NO DATA / NO DATA)")
  }

  test("no data line suppressed") {
    val input = List(
      sources(ds("a", "http://atlas/graph?q=name,latency,:eq,:dist-avg")),
      TimeGroup(0L, step, Map.empty)
    )
    val output = run(input, noDataMsgs = false)
    assertEquals(output.size, 0)
  }

  private def isTimeSeries(messageEnvelope: MessageEnvelope): Boolean = {
    messageEnvelope.message match {
      case _: TimeSeriesMessage => true
      case _                    => false
    }
  }

  private def isEvalDataRate(messageEnvelope: MessageEnvelope): Boolean = {
    messageEnvelope.message match {
      case _: EvalDataRate => true
      case _               => false
    }
  }

  private def getAsEvalDataRate(
    env: MessageEnvelope
  ): EvalDataRate = {
    env.message.asInstanceOf[EvalDataRate]
  }

  private def checkRate(
    rate: EvalDataRate,
    timestamp: Long,
    step: Long,
    inputSize: EvalDataSize,
    intermediateSize: EvalDataSize,
    outputSize: EvalDataSize
  ): Unit = {
    assertEquals(rate.timestamp, timestamp)
    assertEquals(rate.step, step)
    assertEquals(rate.inputSize, inputSize)
    assertEquals(rate.intermediateSize, intermediateSize)
    assertEquals(rate.outputSize, outputSize)
  }

  private def getValue(ts: TimeSeriesMessage): Double = {
    // https://github.com/lampepfl/dotty/issues/15661 ?
    // On 3.4.0 there is an error if using `v` instead of `null` for the case
    // where it is not ArrayData.
    ts.data match {
      case ArrayData(vs) =>
        assertEquals(vs.length, 1)
        vs(0)
      case null =>
        fail(s"unexpected data value: null")
    }
  }

  private def checkValue(ts: TimeSeriesMessage, expected: Double): Unit = {
    val v = getValue(ts)
    if (expected.isNaN)
      assert(v.isNaN)
    else
      assertEqualsDouble(v, expected, 1e-9)
  }

  test("aggregate with single datapoint per group") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, step, expr, "i-1", tags, 42.0)),
      group(2, AggrDatapoint(0, step, expr, "i-1", tags, 43.0)),
      group(3, AggrDatapoint(0, step, expr, "i-1", tags, 44.0))
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 4)
    val expectedTimeseries = List(Double.NaN, 42.0, 43.0, 44.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assertEquals(env.id, "a")
        val ts = env.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }

    val dataRateMsgs = output.filter(isEvalDataRate).filter(_.id == "a")
    assert(dataRateMsgs.size == 3)
    val expectedSizes = Array(
      Array(
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      )
    )
    dataRateMsgs.zipWithIndex.foreach(envAndIndex => {
      val rate = getAsEvalDataRate(envAndIndex._1)
      val i = envAndIndex._2
      checkRate(
        rate,
        60000 * (i + 1),
        60000,
        expectedSizes(i)(0),
        expectedSizes(i)(1),
        expectedSizes(i)(2)
      )
    })
  }

  test("aggregate with multiple datapoints per group") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, step, expr, "i-1", tags, 42.0)),
      group(
        2,
        AggrDatapoint(0, step, expr, "i-1", tags, 43.0),
        AggrDatapoint(0, step, expr, "i-2", tags, 41.0),
        AggrDatapoint(0, step, expr, "i-3", tags, 45.0)
      ),
      group(
        3,
        AggrDatapoint(0, step, expr, "i-1", tags, 43.0),
        AggrDatapoint(0, step, expr, "i-2", tags, 44.0)
      )
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 4)
    val expectedTimeseries = List(Double.NaN, 42.0, 129.0, 87.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assertEquals(env.id, "a")
        val ts = env.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }

    val dataRateMsgs = output.filter(isEvalDataRate).filter(_.id == "a")
    assert(dataRateMsgs.size == 3)
    val expectedSizes = Array(
      Array(
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(3, Map(expr.toString -> 3)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(2, Map(expr.toString -> 2)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      )
    )
    dataRateMsgs.zipWithIndex.foreach(envAndIndex => {
      val rate = getAsEvalDataRate(envAndIndex._1)
      val i = envAndIndex._2
      checkRate(
        rate,
        60000 * (i + 1),
        60000,
        expectedSizes(i)(0),
        expectedSizes(i)(1),
        expectedSizes(i)(2)
      )
    })
  }

  test("aggregate with multiple expressions") {
    val expr1 = DataExpr.Sum(Query.Equal("name", "rps"))
    val expr2 = DataExpr.Max(Query.Equal("name", "gc.pause"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(
        ds("a", s"http://atlas/graph?q=$expr1"),
        ds("b", s"http://atlas/graph?q=$expr2")
      ),
      group(0, AggrDatapoint(0, step, expr1, "i-1", tags, 42.0)),
      group(
        1,
        AggrDatapoint(0, step, expr1, "i-1", tags, 43.0),
        AggrDatapoint(0, step, expr1, "i-2", tags, 41.0),
        AggrDatapoint(0, step, expr2, "i-1", tags, 45.0)
      ),
      group(
        2,
        AggrDatapoint(0, step, expr2, "i-1", tags, 43.0),
        AggrDatapoint(0, step, expr2, "i-3", tags, 49.0),
        AggrDatapoint(0, step, expr1, "i-2", tags, 44.0)
      )
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 3 + 3) // 3 for expr1, 3 for expr2

    val expectedTimeseries1 = scala.collection.mutable.Queue(42.0, 84.0, 44.0)
    val expectedTimeseries2 = scala.collection.mutable.Queue(Double.NaN, 45.0, 49.0)
    timeseries.foreach { env =>
      val actual = env.message.asInstanceOf[TimeSeriesMessage]
      if (env.id == "a")
        checkValue(actual, expectedTimeseries1.dequeue())
      else
        checkValue(actual, expectedTimeseries2.dequeue())
    }

    val expr1DataRateMsgs = output.filter(isEvalDataRate).filter(_.id == "a")
    assert(expr1DataRateMsgs.size == 3)
    val expr1ExpectedSizes = Array(
      Array(
        EvalDataSize(1, Map(expr1.toString -> 1)),
        EvalDataSize(1, Map(expr1.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(2, Map(expr1.toString -> 2)),
        EvalDataSize(1, Map(expr1.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(1, Map(expr1.toString -> 1)),
        EvalDataSize(1, Map(expr1.toString -> 1)),
        EvalDataSize(1)
      )
    )
    expr1DataRateMsgs.zipWithIndex.foreach(envAndIndex => {
      val rate = getAsEvalDataRate(envAndIndex._1)
      val i = envAndIndex._2
      checkRate(
        rate,
        60000 * i,
        60000,
        expr1ExpectedSizes(i)(0),
        expr1ExpectedSizes(i)(1),
        expr1ExpectedSizes(i)(2)
      )
    })

    val expr2DataRateMsgs = output.filter(isEvalDataRate).filter(_.id == "b")
    assert(expr2DataRateMsgs.size == 2)
    val expr2ExpectedSizes = Array(
      Array(
        EvalDataSize(1, Map(expr2.toString -> 1)),
        EvalDataSize(1, Map(expr2.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(2, Map(expr2.toString -> 2)),
        EvalDataSize(1, Map(expr2.toString -> 1)),
        EvalDataSize(1)
      )
    )
    expr2DataRateMsgs.zipWithIndex.foreach(envAndIndex => {
      val rate = getAsEvalDataRate(envAndIndex._1)
      val i = envAndIndex._2
      checkRate(
        rate,
        60000 * (i + 1),
        60000,
        expr2ExpectedSizes(i)(0),
        expr2ExpectedSizes(i)(1),
        expr2ExpectedSizes(i)(2)
      )
    })
  }

  // https://github.com/Netflix/atlas/issues/693
  test("group by with binary operation") {
    val expr1 = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "rps")), List("node"))
    val expr2 = DataExpr.GroupBy(DataExpr.Count(Query.Equal("name", "rps")), List("node"))
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr1,$expr2,:div")),
      group(
        0, // Missing sum for i-2
        AggrDatapoint(0, step, expr1, "i-1", Map("node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr2, "i-1", Map("node" -> "i-1"), 1.0),
        AggrDatapoint(0, step, expr2, "i-2", Map("node" -> "i-2"), 1.0)
      ),
      group(
        1,
        AggrDatapoint(0, step, expr1, "i-1", Map("node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr1, "i-2", Map("node" -> "i-2"), 21.0),
        AggrDatapoint(0, step, expr2, "i-1", Map("node" -> "i-1"), 1.0),
        AggrDatapoint(0, step, expr2, "i-2", Map("node" -> "i-2"), 1.0)
      ),
      group(
        2, // Missing count for i-1
        AggrDatapoint(0, step, expr1, "i-1", Map("node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr1, "i-2", Map("node" -> "i-2"), 21.0),
        AggrDatapoint(0, step, expr2, "i-2", Map("node" -> "i-2"), 1.0)
      )
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 4)
    timeseries.foreach { env =>
      val ts = env.message.asInstanceOf[TimeSeriesMessage]
      if (ts.tags("node") == "i-1") {
        assert(ts.start < 120000)
        checkValue(ts, 42.0)
      } else {
        assert(ts.start > 0)
        checkValue(ts, 21.0)
      }
    }

    val dataRateMsgs = output.filter(isEvalDataRate).filter(_.id == "a")
    assert(dataRateMsgs.size == 3)
    val expectedSizes = Array(
      Array(
        EvalDataSize(3, Map(expr1.toString -> 1, expr2.toString -> 2)),
        EvalDataSize(3, Map(expr1.toString -> 1, expr2.toString -> 2)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(4, Map(expr1.toString -> 2, expr2.toString -> 2)),
        EvalDataSize(4, Map(expr1.toString -> 2, expr2.toString -> 2)),
        EvalDataSize(2)
      ),
      Array(
        EvalDataSize(3, Map(expr1.toString -> 2, expr2.toString -> 1)),
        EvalDataSize(3, Map(expr1.toString -> 2, expr2.toString -> 1)),
        EvalDataSize(1)
      )
    )
    dataRateMsgs.zipWithIndex.foreach(envAndIndex => {
      val rate = getAsEvalDataRate(envAndIndex._1)
      val i = envAndIndex._2
      checkRate(
        rate,
        60000 * i,
        60000,
        expectedSizes(i)(0),
        expectedSizes(i)(1),
        expectedSizes(i)(2)
      )
    })
  }

  test("empty aggregated group by with binary operation") {
    val expr1 = DataExpr.GroupBy(DataExpr.Max(Query.Equal("name", "a")), List("node"))
    val expr2 = DataExpr.GroupBy(DataExpr.Max(Query.Equal("name", "b")), List("node"))
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr1,:sum,$expr2,:sum,:add")),
      group(
        0, // No data for expr1
        AggrDatapoint(0, step, expr2, "i-1", Map("name" -> "b"), 1.0)
      ),
      group(
        1,
        AggrDatapoint(0, step, expr2, "i-1", Map("name" -> "b"), 1.0)
      )
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 2)
    timeseries.foreach { env =>
      val ts = env.message.asInstanceOf[TimeSeriesMessage]
      checkValue(ts, 1.0)
    }
  }

  test("empty aggregated percentile approximation with binary operation") {
    val expr1 = MathExpr.Percentiles(
      DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "a")), List("percentile")),
      List(90.0)
    )
    val expr2 = MathExpr.Percentiles(
      DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "b")), List("percentile")),
      List(90.0)
    )
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr1,:sum,$expr2,:sum,:add")),
      group(
        0, // No data for expr1
        AggrDatapoint(0, step, expr2.expr, "i-1", Map("name" -> "b", "percentile" -> "T0000"), 1.0)
      ),
      group(
        1,
        AggrDatapoint(0, step, expr2.expr, "i-1", Map("name" -> "b", "percentile" -> "T0000"), 1.0)
      )
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 2)
    timeseries.foreach { env =>
      val ts = env.message.asInstanceOf[TimeSeriesMessage]
      checkValue(ts, 9e-10)
    }
  }

  test("multi-level group by with stateful operation") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "rps")), List("node", "app"))
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr,2,:rolling-max,:max,(,app,),:by")),
      group(
        0,
        AggrDatapoint(0, step, expr, "i-1", Map("app" -> "foo", "node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr, "i-2", Map("app" -> "foo", "node" -> "i-2"), 1.0)
      ),
      group(
        1,
        AggrDatapoint(0, step, expr, "i-1", Map("app" -> "foo", "node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr, "i-2", Map("app" -> "foo", "node" -> "i-2"), 21.0)
      ),
      group(
        2,
        AggrDatapoint(0, step, expr, "i-1", Map("app" -> "foo", "node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr, "i-2", Map("app" -> "foo", "node" -> "i-2"), 84.0)
      ),
      group(
        3,
        AggrDatapoint(0, step, expr, "i-1", Map("app" -> "foo", "node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr, "i-2", Map("app" -> "foo", "node" -> "i-2"), 21.0)
      ),
      group(
        4,
        AggrDatapoint(0, step, expr, "i-1", Map("app" -> "foo", "node" -> "i-1"), 21.0),
        AggrDatapoint(0, step, expr, "i-2", Map("app" -> "foo", "node" -> "i-2"), 21.0)
      ),
      group(
        5,
        AggrDatapoint(0, step, expr, "i-1", Map("app" -> "foo", "node" -> "i-1"), 21.0),
        AggrDatapoint(0, step, expr, "i-2", Map("app" -> "foo", "node" -> "i-2"), 21.0)
      )
    )

    val output = run(input)

    val expected = Array(42.0, 42.0, 84.0, 84.0, 42.0, 21.0)
    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 6)
    timeseries.zip(expected).foreach {
      case (envelope, expectedValue) =>
        val ts = envelope.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }
  }

  test("multi-level aggr tagging") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "rps")), List("node", "app"))
    val baseTags = Map("name" -> "rps", "app" -> "foo")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr,:max")),
      group(
        0,
        AggrDatapoint(0, step, expr, "i-1", baseTags + ("node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr, "i-2", baseTags + ("node" -> "i-2"), 1.0)
      ),
      group(
        1,
        AggrDatapoint(0, step, expr, "i-1", baseTags + ("node" -> "i-1"), 42.0),
        AggrDatapoint(0, step, expr, "i-2", baseTags + ("node" -> "i-2"), 43.0)
      )
    )

    val output = run(input)

    val expected = Array(42.0, 43.0)
    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 2)
    timeseries.zip(expected).foreach {
      case (envelope, expectedValue) =>
        val ts = envelope.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
        assertEquals(ts.tags, Map("name" -> "rps", "atlas.offset" -> "0w"))
    }
  }

  // https://github.com/Netflix/atlas/issues/762
  test(":legend is honored") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr,legend+for+$$name,:legend")),
      group(0),
      group(1, AggrDatapoint(0, step, expr, "i-1", tags, 42.0)),
      group(2, AggrDatapoint(0, step, expr, "i-1", tags, 43.0)),
      group(3, AggrDatapoint(0, step, expr, "i-1", tags, 44.0))
    )

    val output = run(input)

    val timeseries = output.filter(_.message.isInstanceOf[TimeSeriesMessage])
    assertEquals(timeseries.size, 4)
    // tail to ignore initial no data entry
    timeseries.tail.foreach { env =>
      val ts = env.message.asInstanceOf[TimeSeriesMessage]
      assertEquals(ts.label, "legend for rps")
    }
  }

  test("state is isolated for duplicate stateful expressions") {
    val exprA = DataExpr.Sum(Query.Equal("name", "a"))
    val expr = MathExpr.And(
      MathExpr.LessThanEqual(StatefulExpr.Derivative(exprA), MathExpr.Constant(0)),
      MathExpr.GreaterThanEqual(StatefulExpr.Derivative(exprA), MathExpr.Constant(0))
    )
    val tagsA = Map("name" -> "a")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, step, exprA, "i-1", tagsA, 6.0)),
      group(2, AggrDatapoint(0, step, exprA, "i-1", tagsA, 5.0)),
      group(3, AggrDatapoint(0, step, exprA, "i-1", tagsA, 4.0))
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 4)
    val expectedTimeseries = List(0.0, 0.0, 0.0, 0.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assertEquals(env.id, "a")
        val ts = env.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }
  }

  test("state maintained on datasource refresh") {
    val exprA = DataExpr.Sum(Query.Equal("name", "a"))
    val expr = MathExpr.Add(
      StatefulExpr.Derivative(exprA),
      StatefulExpr.Derivative(exprA)
    )
    val tagsA = Map("name" -> "a")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, step, exprA, "i-1", tagsA, 6.0)),
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(2, AggrDatapoint(0, step, exprA, "i-1", tagsA, 5.0)),
      group(3, AggrDatapoint(0, step, exprA, "i-1", tagsA, 5.0)),
      group(3, AggrDatapoint(0, step, exprA, "i-1", tagsA, 3.0))
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 5)
    val expectedTimeseries = List(Double.NaN, Double.NaN, -2.0, 0.0, -4.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assertEquals(env.id, "a")
        val ts = env.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }
  }

  test("mixed step sizes should fail") {
    val input = List(
      sources(
        ds("a", s"http://atlas/graph?q=name,rps,:eq,:sum"),
        ds("b", s"http://atlas/graph?q=name,rps,:eq,:sum", 10000L)
      )
    )

    intercept[IllegalStateException] {
      run(input)
    }
    // Message order can be inconsistent, need to address to avoid flakey tests
    // assertEquals(
    //  e.getMessage,
    //  "inconsistent step sizes, expected 60000, found 10000 " +
    //    "on DataSource[id=b, step=PT10S, uri=http://atlas/graph?q=name,rps,:eq,:sum]"
    // )
  }

  test("stateful windows move even if there is no data for expr") {
    val exprA = DataExpr.Sum(Query.Equal("name", "a"))
    val expr = StatefulExpr.RollingCount(exprA, 3)
    val tagsA = Map("name" -> "a")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, step, exprA, "i-1", tagsA, 6.0)),
      group(2),
      group(3, AggrDatapoint(0, step, exprA, "i-1", tagsA, 5.0)),
      group(4),
      group(5),
      group(6),
      group(7, AggrDatapoint(0, step, exprA, "i-1", tagsA, 4.0))
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 8)
    val expectedTimeseries = List(0.0, 1.0, 1.0, 2.0, 1.0, 1.0, 0.0, 1.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assertEquals(env.id, "a")
        val ts = env.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }
  }

  test("stateful windows move even if there is no data for expr grouping") {
    val exprA = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "a")), List("k"))
    val expr = MathExpr.GreaterThan(
      StatefulExpr.RollingCount(
        MathExpr.LessThan(
          MathExpr.Add(exprA, MathExpr.Constant(0.0)),
          MathExpr.Constant(1.0)
        ),
        5
      ),
      MathExpr.Constant(3.5)
    )
    val tagsA = Map("name" -> "a", "k" -> "v")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr")),
      group(0),
      group(1, AggrDatapoint(0, step, exprA, "i-1", tagsA, 6.0)),
      group(2),
      group(3, AggrDatapoint(0, step, exprA, "i-1", tagsA, 5.0)),
      group(4),
      group(5, AggrDatapoint(0, step, exprA, "i-1", tagsA, 4.0)),
      group(6),
      group(7, AggrDatapoint(0, step, exprA, "i-1", tagsA, 4.0))
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 8)
    timeseries.foreach { env =>
      val ts = env.message.asInstanceOf[TimeSeriesMessage]
      val v = getValue(ts)
      if (ts.label == "NO DATA")
        assert(v.isNaN)
      else
        assertEquals(v, 0.0)
    }
  }

  test("presentation metadata, explicit color") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/graph?q=$expr,f00,:color,:stack&hints=presentation-metadata")),
      group(1, AggrDatapoint(0, step, expr, "i-1", tags, 42.0))
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 1)
    val ts = timeseries.head.message().asInstanceOf[TimeSeriesMessage]
    assertEquals(ts.styleMetadata.get.color, Color.RED)
    assertEquals(ts.styleMetadata.get.lineStyle, LineStyle.STACK)
  }

  test("trace time series: aggregate with single datapoint per group") {
    val expr = DataExpr.Sum(Query.Equal("name", "rps"))
    val tags = Map("name" -> "rps")
    val input = List(
      sources(ds("a", s"http://atlas/traces/graph?q=app,foo,:eq,$expr,:span-time-series")),
      group(0),
      group(1, AggrDatapoint(0, step, expr, "i-1", tags, 42.0)),
      group(2, AggrDatapoint(0, step, expr, "i-1", tags, 43.0)),
      group(3, AggrDatapoint(0, step, expr, "i-1", tags, 44.0))
    )

    val output = run(input)

    val timeseries = output.filter(isTimeSeries)
    assertEquals(timeseries.size, 4)
    val expectedTimeseries = List(Double.NaN, 42.0, 43.0, 44.0)
    timeseries.zip(expectedTimeseries).foreach {
      case (env, expectedValue) =>
        assertEquals(env.id, "a")
        val ts = env.message.asInstanceOf[TimeSeriesMessage]
        checkValue(ts, expectedValue)
    }

    val dataRateMsgs = output.filter(isEvalDataRate).filter(_.id == "a")
    assert(dataRateMsgs.size == 3)
    val expectedSizes = Array(
      Array(
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      ),
      Array(
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1, Map(expr.toString -> 1)),
        EvalDataSize(1)
      )
    )
    dataRateMsgs.zipWithIndex.foreach(envAndIndex => {
      val rate = getAsEvalDataRate(envAndIndex._1)
      val i = envAndIndex._2
      checkRate(
        rate,
        60000 * (i + 1),
        60000,
        expectedSizes(i)(0),
        expectedSizes(i)(1),
        expectedSizes(i)(2)
      )
    })
  }
}
