/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.core.model

import java.time.Duration
import java.util.stream.Collectors
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Features
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Gauge
import com.netflix.spectator.api.patterns.DistinctCountSketch
import munit.FunSuite

class ApproxDistinctSuite extends FunSuite {

  private val interpreter = Interpreter(MathVocabulary.allWords)

  private val start = 0L
  private val step = 60000L
  private val context = EvalContext(start, start + step * 2, step)

  private val registers = DistinctCountSketch.REGISTERS

  // Build a register series for register `idx` (encoded as the R## tag) with a constant value
  // across the eval window. Extra tags allow testing group by.
  private def ts(
    idx: Int,
    value: Double,
    extraTags: Map[String, String] = Map.empty
  ): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(value, value))
    val tags = Map("name" -> "test", "statistic" -> "distinct", "distinct" -> f"R$idx%02X")
    TimeSeries(tags ++ extraTags, seq)
  }

  private def parseExpr(str: String): TimeSeriesExpr = {
    // :approx-distinct is not yet a stable word, so enable unstable features.
    interpreter.execute(str, Map.empty[String, Any], Features.UNSTABLE).stack match {
      case (v: TimeSeriesExpr) :: _ => v
      case _                        => throw new IllegalArgumentException("invalid expr")
    }
  }

  private def eval(str: String, input: List[TimeSeries]): List[TimeSeries] = {
    val expr = parseExpr(str)
    // Verify we can reparse the string representation and get an identical expression.
    val e2 = parseExpr(expr.toString)
    val e3 = parseExpr(e2.toString)
    assertEquals(e2, e3)
    expr.eval(context, input).data
  }

  // Record the given distinct values into a real sketch and return the published register
  // series along with the client side cardinality estimate for the same registers.
  private def sketchInput(
    values: Iterable[Long],
    extraTags: Map[String, String] = Map.empty
  ): (List[TimeSeries], Double) = {
    import scala.jdk.CollectionConverters.*
    val r = new DefaultRegistry()
    val sketch = DistinctCountSketch.get(r, r.createId("test"))
    values.foreach(v => sketch.record(v))
    val gauges = r.gauges.collect(Collectors.toList[Gauge]).asScala.toList
    val input = gauges.map { g =>
      val v = g.value()
      val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(v, v))
      val tags = g.id.tags.asScala.map(t => t.key -> t.value).toMap + ("name" -> g.id.name)
      TimeSeries(tags ++ extraTags, seq)
    }
    (input, sketch.cardinality())
  }

  test("estimate matches client for a single interval") {
    val (input, expected) = sketchInput(0L until 1000L)
    val data = eval("name,test,:eq,:approx-distinct", input)

    assertEquals(data.size, 1)
    val t = data.head
    assertEquals(t.label, "approx-distinct(name=test)")
    // Backend must produce exactly the same estimate as the client for the merged registers.
    assertEqualsDouble(t.data(0L), expected, 1e-9)
    assertEqualsDouble(t.data(step), expected, 1e-9)
    // Sanity check the estimate is within a few sigma of the true cardinality (sigma ~13%).
    assertEqualsDouble(t.data(0L), 1000.0, 400.0)
  }

  test("estimate merges registers across sources") {
    // Two sources covering disjoint value ranges. Merging registers (max per register) and
    // estimating should approximate the union (2000 distinct), not either half alone.
    val (a, _) = sketchInput(0L until 1000L)
    val (b, _) = sketchInput(1000L until 2000L)
    val data = eval("name,test,:eq,:approx-distinct", a ::: b)

    assertEquals(data.size, 1)
    assertEqualsDouble(data.head.data(0L), 2000.0, 800.0)
  }

  test("group by another dimension") {
    val (a, ea) = sketchInput(0L until 1000L, Map("region" -> "us-east-1"))
    val (b, eb) = sketchInput(0L until 100L, Map("region" -> "us-west-2"))
    val data = eval("name,test,:eq,(,region,),:by,:approx-distinct", a ::: b)

    assertEquals(data.size, 2)
    val byRegion = data.map(t => t.tags("region") -> t).toMap
    assertEquals(byRegion.keySet, Set("us-east-1", "us-west-2"))
    assert(!byRegion("us-east-1").tags.contains("distinct"))
    assertEqualsDouble(byRegion("us-east-1").data(0L), ea, 1e-9)
    assertEqualsDouble(byRegion("us-west-2").data(0L), eb, 1e-9)
    assertEquals(byRegion("us-east-1").label, "(region=us-east-1)")
  }

  test("all-unset registers are no data (NaN), not zero") {
    // A real published register always has rho >= 1, so an interval where every register is 0
    // means the sketch was absent for that interval. It must read as no data (NaN), not a
    // spurious 0 distinct values, matching how :percentiles and the aggregates report gaps.
    val input = (0 until registers).map(i => ts(i, 0.0)).toList
    val data = eval("name,test,:eq,:approx-distinct", input)
    assertEquals(data.size, 1)
    assert(data.head.data(0L).isNaN)
  }

  test("no-data interval reads as NaN when other intervals have data") {
    // Register set in interval 0, all NaN in interval 1 (source stopped publishing).
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, Array(5.0, Double.NaN))
    val tags = Map("name" -> "test", "statistic" -> "distinct", "distinct" -> "R03")
    val data = eval("name,test,:eq,:approx-distinct", List(TimeSeries(tags, seq)))
    assertEquals(data.size, 1)
    assert(data.head.data(0L) > 0.0)
    assert(data.head.data(step).isNaN)
  }

  test("no matching data") {
    val data = eval("name,test,:eq,:approx-distinct", Nil)
    assertEquals(data.size, 0)
  }

  test("NaN register values treated as unset") {
    // A single register set to a large rho, the rest NaN. Should match feeding the same
    // registers (with NaN as 0) to the client estimator.
    val rhoArray = new Array[Double](registers)
    rhoArray(3) = 5.0
    val expected = DistinctCountSketch.cardinality(rhoArray)

    val input = ts(3, 5.0) :: (0 until registers)
      .filter(_ != 3)
      .map(i => ts(i, Double.NaN))
      .toList
    val data = eval("name,test,:eq,:approx-distinct", input)
    assertEqualsDouble(data.head.data(0L), expected, 1e-9)
  }

  test("bad data with duplicate register") {
    val e = intercept[IllegalArgumentException] {
      // R0A and R0a decode to the same register index (10).
      val bad = List(
        TimeSeries(
          Map("name" -> "test", "statistic" -> "distinct", "distinct" -> "R0A"),
          new ArrayTimeSeq(DsType.Gauge, start, step, Array(1.0, 1.0))
        ),
        TimeSeries(
          Map("name" -> "test", "statistic" -> "distinct", "distinct" -> "R0a"),
          new ArrayTimeSeq(DsType.Gauge, start, step, Array(1.0, 1.0))
        )
      )
      eval("name,test,:eq,:approx-distinct", bad)
    }
    assertEquals(e.getMessage, "requirement failed: invalid distinct encoding: [R0A,R0a]")
  }

  test("round trip toString") {
    // The operator renders as <input>,:approx-distinct, so the input aggregate is shown. The
    // important property is that it re-parses to an equal expression.
    val e1 = parseExpr("name,test,:eq,:approx-distinct")
    assertEquals(e1.toString, "name,test,:eq,:sum,:approx-distinct")
    assertEquals(parseExpr(e1.toString), e1)

    val e2 = parseExpr("name,test,:eq,(,region,),:by,:approx-distinct")
    assertEquals(e2.toString, "name,test,:eq,:sum,(,region,),:by,:approx-distinct")
    assertEquals(parseExpr(e2.toString), e2)
  }

  test("forces max aggregate regardless of input aggregate") {
    // Even if the user writes :sum, the fetched data is grouped by the register key with :max.
    val expr = parseExpr("name,test,:eq,:sum,:approx-distinct")
    expr.dataExprs.head match {
      case gb: DataExpr.GroupBy =>
        assert(gb.af.isInstanceOf[DataExpr.Max])
        assert(gb.keys.contains(TagKey.distinct))
      case other => fail(s"expected register group by, got $other")
    }
  }

  test("offset round trips and reaches the register data expr") {
    val e = parseExpr("name,test,:eq,:approx-distinct,1h,:offset")
    assertEquals(parseExpr(e.toString), e)
    assertEquals(e.dataExprs.head.offset, Duration.ofHours(1))
  }

  test("group by applied after the operator") {
    // The input is kept as provided, so a `:by` after the operator groups the underlying
    // aggregate and the register grouping is re-derived; the surviving group key is preserved.
    val e = parseExpr("name,test,:eq,:approx-distinct,(,region,),:by")
    assertEquals(e.finalGrouping, List("region"))
    assertEquals(parseExpr(e.toString), e)
    e.dataExprs.head match {
      case gb: DataExpr.GroupBy => assertEquals(gb.keys, List(TagKey.distinct, "region"))
      case other                => fail(s"expected register group by, got $other")
    }
  }
}
