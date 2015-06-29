/*
 * Copyright 2015 Netflix, Inc.
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

import org.scalatest.FunSuite


class PlotDefSuite extends FunSuite {


  test("lower >= upper") {
    intercept[IllegalArgumentException] {
      PlotDef(Nil, lower = Some(2.0), upper = Some(1.0))
    }
  }

  test("finalBounds explicit") {
    val plotDef = PlotDef(Nil, lower = Some(1.0), upper = Some(42.0))
    assert(plotDef.finalBounds(false, 0.0, 43.0) === 1.0 -> 42.0)
  }

  test("finalBounds explicit lower, auto upper") {
    val plotDef = PlotDef(Nil, lower = Some(1.0), upper = None)
    assert(plotDef.finalBounds(false, 0.0, 43.0) === 1.0 -> 43.0)
  }

  test("finalBounds auto lower, explict upper") {
    val plotDef = PlotDef(Nil, lower = None, upper = Some(42.0))
    assert(plotDef.finalBounds(false, 0.0, 43.0) === 0.0 -> 42.0)
  }

  test("finalBounds auto") {
    val plotDef = PlotDef(Nil, lower = None, upper = None)
    assert(plotDef.finalBounds(false, 0.0, 43.0) === 0.0 -> 43.0)
  }

  test("finalBounds constant, auto") {
    val plotDef = PlotDef(Nil, lower = None, upper = None)
    assert(plotDef.finalBounds(false, 0.0, 0.0) === 0.0 -> 1.0)
  }

  test("finalBounds constant, explicit lower") {
    val plotDef = PlotDef(Nil, lower = Some(0.0), upper = None)
    assert(plotDef.finalBounds(false, 0.0, 0.0) === 0.0 -> 1.0)
  }

  test("finalBounds constant, explicit lower > auto upper") {
    val plotDef = PlotDef(Nil, lower = Some(1.0), upper = None)
    assert(plotDef.finalBounds(false, 0.0, 0.0) === 1.0 -> 2.0)
  }

  test("finalBounds constant, explicit upper") {
    val plotDef = PlotDef(Nil, lower = None, upper = Some(42.0))
    assert(plotDef.finalBounds(false, 0.0, 0.0) === 0.0 -> 42.0)
  }

  test("finalBounds constant, explicit upper == auto lower") {
    val plotDef = PlotDef(Nil, lower = None, upper = Some(0.0))
    assert(plotDef.finalBounds(false, 0.0, 0.0) === -1.0 -> 0.0)
  }

  test("finalBounds constant, explicit upper < auto lower") {
    val plotDef = PlotDef(Nil, lower = None, upper = Some(-1.0))
    assert(plotDef.finalBounds(false, 0.0, 0.0) === -2.0 -> -1.0)
  }

  test("finalBounds area, auto") {
    val plotDef = PlotDef(Nil, lower = None, upper = None)
    assert(plotDef.finalBounds(true, 2.0, 2.0) === 0.0 -> 2.0)
  }

  test("finalBounds area, auto spans 0") {
    val plotDef = PlotDef(Nil, lower = None, upper = None)
    assert(plotDef.finalBounds(true, -42.0, 42.0) === -42.0 -> 42.0)
  }

  test("finalBounds area, auto lower > 0") {
    val plotDef = PlotDef(Nil, lower = None, upper = None)
    assert(plotDef.finalBounds(true, 50.0, 55.0) === 0.0 -> 55.0)
  }

  test("finalBounds area, auto upper < 0") {
    val plotDef = PlotDef(Nil, lower = None, upper = None)
    assert(plotDef.finalBounds(true, -50.0, -45.0) === -50.0 -> 0.0)
  }

  test("finalBounds area, explicit lower > 0") {
    val plotDef = PlotDef(Nil, lower = Some(1.0), upper = None)
    assert(plotDef.finalBounds(true, 2.0, 2.0) === 1.0 -> 2.0)
  }

  test("finalBounds area, explicit upper < 0") {
    val plotDef = PlotDef(Nil, lower = None, upper = Some(-42.0))
    assert(plotDef.finalBounds(true, -50.0, -45.0) === -50.0 -> -42.0)
  }
}
