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
package com.netflix.atlas.core.db

import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

/**
  * The viewer counts are estimates from a sketch with 64 registers, so they are checked
  * against the intended figures with a tolerance rather than exactly. The tolerance is wide
  * enough for the sampling error and narrow enough to catch a mistake in how the ids are laid
  * out, which otherwise shows up only as a plausible looking but wrong number.
  */
class LiveEventViewersSuite extends FunSuite {

  private val interpreter = Interpreter(StyleVocabulary.allWords)
  private val db = StaticDatabase.demo

  private val eventStart = 20L * 3600000L
  private val step = 60000L

  private def at(minutes: Int): Long = eventStart + minutes * step

  private def eval(expr: String, t: Long): List[TimeSeries] = {
    val styleExpr = interpreter
      .execute(expr)
      .stack
      .collect { case ModelDataTypes.PresentationType(e) => e }
      .head
    val ctx = EvalContext(t, t + step, step)
    val data = styleExpr.expr.dataExprs.distinct.map(d => d -> db.execute(ctx, d)).toMap
    styleExpr.expr.eval(ctx, data).data
  }

  private def concurrent(t: Long): Double = {
    eval("name,viewers.concurrent,:eq,:sum,:approx-distinct", t).head.data(t)
  }

  private def assertClose(actual: Double, expected: Double, what: String): Unit = {
    val error = math.abs(actual - expected) / expected
    assert(error < 0.15, f"$what: $actual%.0f is ${error * 100}%.1f%% from $expected%.0f")
  }

  test("baseline audience outside of the event") {
    assertClose(concurrent(at(-30)), 60000, "baseline before")
    assertClose(concurrent(at(230)), 60000, "baseline after")
  }

  test("ramps to a million and holds through the churn") {
    assertClose(concurrent(at(30)), 1000000, "end of ramp")
    List(45, 90, 135).foreach { m =>
      assertClose(concurrent(at(m)), 1000000, s"plateau at $m")
    }
  }

  test("climbs to two million for the main event and holds") {
    assertClose(concurrent(at(180)), 2000000, "peak")
    assertClose(concurrent(at(205)), 2000000, "hold")
  }

  test("drops back to the baseline when the event ends") {
    assert(concurrent(at(215)) < concurrent(at(210)), "should be falling")
    assertClose(concurrent(at(221)), 60000, "back to baseline")
  }

  test("audience is split across the devices") {
    val t = at(180)
    val byDevice = eval("name,viewers.concurrent,:eq,:sum,(,device,),:by,:approx-distinct", t)
    assertEquals(byDevice.size, 3)
    val values = byDevice.map(s => s.label -> s.data(t)).toMap
    assertClose(values("(device=tv)"), 1100000, "tv")
    assertClose(values("(device=phone)"), 600000, "phone")
    assertClose(values("(device=laptop)"), 300000, "laptop")

    // The devices are numbered from different ids, so the total is their union rather than
    // any one of them. If the ranges overlapped the total would come out short.
    assertClose(concurrent(t), values.values.sum, "total against sum of devices")
  }

  test("more people watch over the event than are ever watching at once") {
    val start = eventStart - 30 * step
    val end = eventStart + 230 * step
    val styleExpr = interpreter
      .execute("name,viewers.concurrent,:eq,:sum,:approx-distinct-cumulative")
      .stack
      .collect { case ModelDataTypes.PresentationType(e) => e }
      .head
    val ctx = EvalContext(start, end, step)
    val data = styleExpr.expr.dataExprs.distinct.map(d => d -> db.execute(ctx, d)).toMap
    val cumulative = styleExpr.expr.eval(ctx, data).data.head

    // The churn replaces 5% of the plateau audience every 15 minutes, so the unique audience
    // over the event is larger than the two million watching at the peak.
    val unique = cumulative.data(at(220))
    assert(unique > 2100000, s"unique viewers $unique should exceed the peak")
    assertClose(unique, 2400000, "unique viewers over the event")
  }
}
