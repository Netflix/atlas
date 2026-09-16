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

class ApproxDistinctRollingSuite extends FunSuite {

  private val interpreter = Interpreter(StatefulVocabulary.allWords)

  private val start = 0L
  private val step = 60000L

  private def parseExpr(str: String): TimeSeriesExpr = {
    interpreter.execute(str, Map.empty[String, Any], Features.STABLE).stack match {
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

  test("expands to rolling-max wrapping the register group by") {
    val expr = parseExpr("name,test,:eq,2m,:approx-distinct-rolling")
    val eval = expr match {
      case nr: MathExpr.NamedRewrite => nr.evalExpr
      case other                     => other
    }
    // The estimator wraps a rolling-max over the requested window...
    eval match {
      case MathExpr.ApproxDistinct(rm: StatefulExpr.RollingMax) =>
        assertEquals(rm.window, RollingWindow.Time(java.time.Duration.ofMinutes(2)))
      case other =>
        fail(s"unexpected eval expr: $other")
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
    // The window is part of the display expression, otherwise the rendered expression could
    // not be parsed back.
    assertEquals(
      parseExpr("name,test,:eq,2m,:approx-distinct-rolling").toString,
      "name,test,:eq,:sum,PT2M,:approx-distinct-rolling"
    )
    val grouped = parseExpr("name,test,:eq,(,region,),:by,2m,:approx-distinct-rolling")
    assertEquals(parseExpr(grouped.toString), grouped)
  }

  test("window as a number of datapoints") {
    val expr = parseExpr("name,test,:eq,3,:approx-distinct-rolling")
    assertEquals(expr.toString, "name,test,:eq,:sum,3,:approx-distinct-rolling")
    assertEquals(parseExpr(expr.toString), expr)
  }

  test("group by applied after the operator") {
    val e = parseExpr("name,test,:eq,2m,:approx-distinct-rolling,(,region,),:by")
    assertEquals(e.finalGrouping, List("region"))
    assertEquals(parseExpr(e.toString), e)
  }

  test("rolling unions distinct values within the window") {
    // Three intervals covering disjoint value ranges. A 2m window at a 1m step covers two
    // intervals, so the estimate is the union of the current and previous interval and the
    // first interval drops out of the window by the third.
    val i0 = registers(0L until 1000L)
    val i1 = registers(1000L until 2000L)
    val i2 = registers(2000L until 3000L)
    val input = sketchSeries(Seq(i0, i1, i2))
    val context = EvalContext(start, start + step * 3, step)

    val data = parseExpr("name,test,:eq,2m,:approx-distinct-rolling").eval(context, input).data
    assertEquals(data.size, 1)
    val ts = data.head

    assertEqualsDouble(ts.data(0L), 1000.0, 400.0)
    assertEqualsDouble(ts.data(step), 2000.0, 800.0)
    // Third interval only unions i1 and i2, so it stays at ~2000 rather than growing to 3000.
    assertEqualsDouble(ts.data(2 * step), 2000.0, 800.0)
  }

  test("rolling window carries across chunked evaluations") {
    // Streaming and /api/v1/fetch evaluate one chunk at a time and thread the stateful buffers
    // through the eval context. If the state is dropped the rolling max restarts every chunk
    // and the window is effectively a single interval.
    val i0 = registers(0L until 1000L)
    val i1 = registers(1000L until 2000L)
    val expr = parseExpr("name,test,:eq,2m,:approx-distinct-rolling")

    val whole = expr
      .eval(EvalContext(start, start + step * 2, step), sketchSeries(Seq(i0, i1)))
      .data
      .head
      .data(step)

    var state = Map.empty[StatefulExpr, Any]
    val chunked = List(i0, i1).zipWithIndex.map {
      case (regs, i) =>
        val t = start + i * step
        // Each chunk only has the data for its own interval, as would be the case for a
        // streaming evaluation.
        val input = (0 until DistinctCountSketch.REGISTERS).map { idx =>
          val seq = new ArrayTimeSeq(DsType.Gauge, t, step, Array(regs.getOrElse(idx, 0.0)))
          TimeSeries(
            Map("name" -> "test", "statistic" -> "distinct", "distinct" -> f"R$idx%02X"),
            seq
          )
        }.toList
        val rs = expr.eval(EvalContext(t, t + step, step, state), input)
        state = rs.state
        rs.data.head.data(t)
    }

    assertEquals(chunked.last, whole)
  }

  test("window smaller than the step matches approx-distinct") {
    val i0 = registers(0L until 1000L)
    val i1 = registers(1000L until 2000L)
    val input = sketchSeries(Seq(i0, i1))
    val context = EvalContext(start, start + step * 2, step)

    // 30s window with a 1m step: a single interval already covers the window.
    val rolling = parseExpr("name,test,:eq,30s,:approx-distinct-rolling").eval(context, input).data
    val perInterval = parseExpr("name,test,:eq,:approx-distinct").eval(context, input).data
    assertEquals(rolling.head.data(step), perInterval.head.data(step))
  }
}
