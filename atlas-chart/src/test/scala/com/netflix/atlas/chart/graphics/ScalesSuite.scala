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
package com.netflix.atlas.chart.graphics

import munit.FunSuite

class ScalesSuite extends FunSuite {

  test("linear") {
    val scale = Scales.linear(0.0, 100.0, 0, 100)
    assertEquals(scale(0.0), 0)
    assertEquals(scale(10.0), 10)
    assertEquals(scale(20.0), 20)
    assertEquals(scale(30.0), 30)
    assertEquals(scale(40.0), 40)
    assertEquals(scale(50.0), 50)
    assertEquals(scale(60.0), 60)
    assertEquals(scale(70.0), 70)
    assertEquals(scale(80.0), 80)
    assertEquals(scale(90.0), 90)
    assertEquals(scale(100.0), 100)
  }

  test("ylinear_l1_u2_h300") {
    val scale = Scales.yscale(Scales.linear)(1.0, 2.0, 0, 300)
    assertEquals(scale(1.0), 300)
    assertEquals(scale(2.0), 0)
  }

  test("logarithmic") {
    val scale = Scales.logarithmic(0.0, 100.0, 0, 100)
    assertEquals(scale(0.0), 0)
    assertEquals(scale(10.0), 51)
    assertEquals(scale(20.0), 65)
    assertEquals(scale(30.0), 74)
    assertEquals(scale(40.0), 80)
    assertEquals(scale(50.0), 85)
    assertEquals(scale(60.0), 89)
    assertEquals(scale(70.0), 92)
    assertEquals(scale(80.0), 95)
    assertEquals(scale(90.0), 97)
    assertEquals(scale(100.0), 100)
  }

  test("logarithmic negative") {
    val scale = Scales.logarithmic(-100.0, 0.0, 0, 100)
    assertEquals(scale(0.0), 100)
    assertEquals(scale(-10.0), 48)
    assertEquals(scale(-20.0), 34)
    assertEquals(scale(-30.0), 25)
    assertEquals(scale(-40.0), 19)
    assertEquals(scale(-50.0), 14)
    assertEquals(scale(-60.0), 10)
    assertEquals(scale(-70.0), 7)
    assertEquals(scale(-80.0), 4)
    assertEquals(scale(-90.0), 2)
    assertEquals(scale(-100.0), 0)
  }

  test("logarithmic positive and negative") {
    val scale = Scales.logarithmic(-100.0, 100.0, 0, 100)
    assertEquals(scale(100.0), 100)
    assertEquals(scale(50.0), 92)
    assertEquals(scale(10.0), 75)
    assertEquals(scale(0.0), 50)
    assertEquals(scale(-10.0), 24)
    assertEquals(scale(-50.0), 7)
    assertEquals(scale(-100.0), 0)
  }

  test("logarithmic less than lower bound") {
    val scale = Scales.logarithmic(15.0, 100.0, 0, 100)
    assertEquals(scale(0.0), -150)
    assertEquals(scale(10.0), -20)
    assertEquals(scale(20.0), 14)
    assertEquals(scale(30.0), 35)
    assertEquals(scale(40.0), 51)
    assertEquals(scale(50.0), 62)
    assertEquals(scale(60.0), 72)
    assertEquals(scale(70.0), 80)
    assertEquals(scale(80.0), 88)
    assertEquals(scale(90.0), 94)
    assertEquals(scale(100.0), 100)
  }

  test("logLinear [1e-9, 57.266230611]") {
    val scale = Scales.logLinear(1e-9, 57.266230611, 22, 522)
    assertEquals(scale(1e-9), 27)
    assertEquals(scale(2e-9), 32)
    assertEquals(scale(3e-9), 37)
  }

  test("invertedLogLinear [1, 110]") {
    val scale = Scales.invertedLogLinear(1.0, 110.0, 0, 5)
    assertEquals(scale(0), 0.9)
    assertEquals(scale(1), 4.0)
    assertEquals(scale(5), 200.0)
  }

  test("invertedLogLinear [1234, 1.234e8]") {
    val scale = Scales.invertedLogLinear(1234.0, 1.234e8, 0, 5)
    assertEquals(scale(0), 1000.0)
    assertEquals(scale(1), 10000.0)
    assertEquals(scale(5), 2.0e8)
  }
}
