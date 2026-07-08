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

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Features
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Gauge
import com.netflix.spectator.api.patterns.DistinctCountSketch
import munit.FunSuite

import java.util.stream.Collectors

class ApproxDistinctCumulativeSuite extends FunSuite {

  private val interpreter = Interpreter(StatefulVocabulary.allWords)

  private val start = 0L
  private val step = 60000L

  private def parseExpr(str: String): TimeSeriesExpr = {
    interpreter.execute(str, Map.empty[String, Any], Features.UNSTABLE).stack match {
      case (v: TimeSeriesExpr) :: _ => v
      case _                        => throw new IllegalArgumentException("invalid expr")
    }
  }

  // Record the given values into a sketch and return the published register value (max rho) by
  // register index.
  private def registers(values: Iterable[Long]): Map[Int, Double] = {
    import scala.jdk.CollectionConverters.*
    val r = new DefaultRegistry()
    val sketch = DistinctCountSketch.get(r, r.createId("test"))
    values.foreach(v => sketch.record(v))
    r.gauges
      .collect(Collectors.toList[Gauge])
      .asScala
      .map(g =>
        Integer.parseInt(
          g.id.tags.asScala.find(_.key == "distinct").get.value.substring(1),
          16
        ) -> g.value()
      )
      .toMap
  }

  // Build the 64 register series, where interval i takes its rho values from perInterval(i).
  private def sketchSeries(perInterval: Seq[Map[Int, Double]]): List[TimeSeries] = {
    (0 until DistinctCountSketch.REGISTERS).map { idx =>
      val values = perInterval.map(_.getOrElse(idx, 0.0)).toArray
      val seq = new ArrayTimeSeq(DsType.Gauge, start, step, values)
      val tags = Map("name" -> "test", "statistic" -> "distinct", "distinct" -> f"R$idx%02X")
      TimeSeries(tags, seq)
    }.toList
  }

  test("expands to cumulative-max wrapping the register group by") {
    val expr = parseExpr("name,test,:eq,:approx-distinct-cumulative")
    // NamedRewrite display, but the eval expression is ApproxDistinct(CumulativeMax(GroupBy...)).
    val eval = expr match {
      case nr: MathExpr.NamedRewrite => nr.evalExpr
      case other                     => other
    }
    // The estimator wraps a cumulative-max (the running max over time)...
    eval match {
      case MathExpr.ApproxDistinct(_: StatefulExpr.CumulativeMax) => ()
      case other => fail(s"unexpected eval expr: $other")
    }
    // ...and the data it fetches is grouped by the register key with a max aggregate.
    eval.dataExprs.head match {
      case gb: DataExpr.GroupBy =>
        assert(gb.af.isInstanceOf[DataExpr.Max])
        assert(gb.keys.contains(TagKey.distinct))
      case other => fail(s"expected register group by, got $other")
    }
  }

  test("round trip toString") {
    assertEquals(
      parseExpr("name,test,:eq,:approx-distinct-cumulative").toString,
      "name,test,:eq,:approx-distinct-cumulative"
    )
    // Group by is applied before the operator and round trips.
    val grouped = parseExpr("name,test,:eq,(,region,),:by,:approx-distinct-cumulative")
    assertEquals(parseExpr(grouped.toString), grouped)
  }

  test("group by applied after the operator") {
    val e = parseExpr("name,test,:eq,:approx-distinct-cumulative,(,region,),:by")
    assertEquals(e.finalGrouping, List("region"))
    assertEquals(parseExpr(e.toString), e)
  }

  test("raw cumulative-max composition preserves the wrapper in toString") {
    // Regression: :approx-distinct renders as <input>,:approx-distinct, so a wrapping
    // :cumulative-max is preserved rather than dropped (which would have collapsed it to a plain
    // :approx-distinct and silently lost the cumulative behavior on round trip).
    val e = parseExpr("name,test,:eq,:cumulative-max,:approx-distinct")
    assertEquals(e.toString, "name,test,:eq,:sum,:cumulative-max,:approx-distinct")
    assertEquals(parseExpr(e.toString), e)
  }

  test("cumulative unions distinct values across time") {
    // Two intervals covering disjoint value ranges. The cumulative estimate at the second
    // interval should approximate the union (2000), larger than either interval alone (~1000).
    val i0 = registers(0L until 1000L)
    val i1 = registers(1000L until 2000L)
    val input = sketchSeries(Seq(i0, i1))
    val context = EvalContext(start, start + step * 2, step)

    val perInterval = parseExpr("name,test,:eq,:approx-distinct").eval(context, input).data
    val cumulative =
      parseExpr("name,test,:eq,:approx-distinct-cumulative").eval(context, input).data

    assertEquals(perInterval.size, 1)
    assertEquals(cumulative.size, 1)

    // Per-interval: each interval sees ~1000 distinct.
    assertEqualsDouble(perInterval.head.data(0L), 1000.0, 400.0)
    assertEqualsDouble(perInterval.head.data(step), 1000.0, 400.0)

    // Cumulative: interval 0 ~1000, interval 1 ~2000 (union), and non-decreasing.
    assertEqualsDouble(cumulative.head.data(0L), 1000.0, 400.0)
    assertEqualsDouble(cumulative.head.data(step), 2000.0, 800.0)
    assert(cumulative.head.data(step) >= cumulative.head.data(0L))
    // And strictly larger than the per-interval estimate for the second interval.
    assert(cumulative.head.data(step) > perInterval.head.data(step))
  }

  test("group by dimension") {
    val i0 = registers(0L until 500L)
    val i1 = registers(500L until 1000L)
    val series = sketchSeries(Seq(i0, i1)).map { t =>
      TimeSeries(t.tags + ("region" -> "us-east-1"), t.data)
    }
    val context = EvalContext(start, start + step * 2, step)
    val data = parseExpr("name,test,:eq,(,region,),:by,:approx-distinct-cumulative")
      .eval(context, series)
      .data
    assertEquals(data.size, 1)
    assertEquals(data.head.tags.get("region"), Some("us-east-1"))
    assert(!data.head.tags.contains("distinct"))
    // Cumulative over the two disjoint halves approximates the full 1000.
    assertEqualsDouble(data.head.data(step), 1000.0, 400.0)
  }
}
