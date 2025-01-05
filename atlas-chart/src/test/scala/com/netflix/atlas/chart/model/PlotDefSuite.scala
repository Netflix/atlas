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
package com.netflix.atlas.chart.model

import com.netflix.atlas.chart.model.PlotBound.AutoData
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model.PlotBound.Explicit
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

class PlotDefSuite extends FunSuite {

  test("lower >= upper") {
    intercept[IllegalArgumentException] {
      PlotDef(Nil, lower = Explicit(2.0), upper = Explicit(1.0))
    }
  }

  test("finalBounds explicit") {
    val plotDef = PlotDef(Nil, lower = Explicit(1.0), upper = Explicit(42.0))
    assertEquals(plotDef.finalBounds(false, 0.0, 43.0), 1.0 -> 42.0)
  }

  test("finalBounds explicit lower, auto upper") {
    val plotDef = PlotDef(Nil, lower = Explicit(1.0), upper = AutoStyle)
    assertEquals(plotDef.finalBounds(false, 0.0, 43.0), 1.0 -> 43.0)
  }

  test("finalBounds auto lower, explict upper") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = Explicit(42.0))
    assertEquals(plotDef.finalBounds(false, 0.0, 43.0), 0.0 -> 42.0)
  }

  test("finalBounds auto") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(false, 0.0, 43.0), 0.0 -> 43.0)
  }

  test("finalBounds constant, auto") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(false, 0.0, 0.0), 0.0 -> 1.0)
  }

  test("finalBounds constant, explicit lower") {
    val plotDef = PlotDef(Nil, lower = Explicit(0.0), upper = AutoStyle)
    assertEquals(plotDef.finalBounds(false, 0.0, 0.0), 0.0 -> 1.0)
  }

  test("finalBounds constant, explicit lower > auto upper") {
    val plotDef = PlotDef(Nil, lower = Explicit(1.0), upper = AutoStyle)
    assertEquals(plotDef.finalBounds(false, 0.0, 0.0), 1.0 -> 2.0)
  }

  test("finalBounds constant, explicit upper") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = Explicit(42.0))
    assertEquals(plotDef.finalBounds(false, 0.0, 0.0), 0.0 -> 42.0)
  }

  test("finalBounds constant, explicit upper == auto lower") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = Explicit(0.0))
    assertEquals(plotDef.finalBounds(false, 0.0, 0.0), -1.0 -> 0.0)
  }

  test("finalBounds constant, explicit upper < auto lower") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = Explicit(-1.0))
    assertEquals(plotDef.finalBounds(false, 0.0, 0.0), -2.0 -> -1.0)
  }

  test("finalBounds area, auto") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(true, 2.0, 2.0), 0.0 -> 2.0)
  }

  test("finalBounds area, auto spans 0") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(true, -42.0, 42.0), -42.0 -> 42.0)
  }

  test("finalBounds area, auto lower > 0") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(true, 50.0, 55.0), 0.0 -> 55.0)
  }

  test("finalBounds area, auto upper < 0") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(true, -50.0, -45.0), -50.0 -> 0.0)
  }

  test("finalBounds area, explicit lower > 0") {
    val plotDef = PlotDef(Nil, lower = Explicit(1.0), upper = AutoStyle)
    assertEquals(plotDef.finalBounds(true, 2.0, 2.0), 1.0 -> 2.0)
  }

  test("finalBounds area, explicit upper < 0") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = Explicit(-42.0))
    assertEquals(plotDef.finalBounds(true, -50.0, -45.0), -50.0 -> -42.0)
  }

  test("finalBounds area, auto-data") {
    val plotDef = PlotDef(Nil, lower = AutoData, upper = AutoData)
    assertEquals(plotDef.finalBounds(true, 2.0, 2.0), 2.0 -> 3.0)
  }

  test("finalBounds area, auto-data spans 0") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(true, -42.0, 42.0), -42.0 -> 42.0)
  }

  test("finalBounds area, auto-data lower > 0") {
    val plotDef = PlotDef(Nil, lower = AutoData, upper = AutoStyle)
    assertEquals(plotDef.finalBounds(true, 50.0, 55.0), 50.0 -> 55.0)
  }

  test("finalBounds area, auto-data upper < 0") {
    val plotDef = PlotDef(Nil, lower = AutoStyle, upper = AutoData)
    assertEquals(plotDef.finalBounds(true, -50.0, -45.0), -50.0 -> -45.0)
  }

  test("bounds infinity") {
    val seq = new ArrayTimeSeq(DsType.Gauge, 0L, 1L, Array(Double.PositiveInfinity))
    val ts = TimeSeries(Map("name" -> "infinity"), seq)
    val plotDef = PlotDef(List(LineDef(ts)))

    val (min, max) = plotDef.bounds(0L, 1L)
    assertEquals(min, 0.0)
    assertEquals(max, 1.0)
  }

  test("bounds infinity, stack") {
    val seq = new ArrayTimeSeq(DsType.Gauge, 0L, 1L, Array(Double.PositiveInfinity))
    val ts = TimeSeries(Map("name" -> "infinity"), seq)
    val plotDef = PlotDef(List(LineDef(ts, lineStyle = LineStyle.STACK)))

    val (min, max) = plotDef.bounds(0L, 1L)
    assertEquals(min, 0.0)
    assertEquals(max, 1.0)
  }
}
