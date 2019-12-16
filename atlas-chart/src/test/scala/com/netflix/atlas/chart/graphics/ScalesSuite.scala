/*
 * Copyright 2014-2019 Netflix, Inc.
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

import org.scalatest.funsuite.AnyFunSuite

class ScalesSuite extends AnyFunSuite {

  test("linear") {
    val scale = Scales.linear(0.0, 100.0, 0, 100)
    assert(scale(0.0) === 0)
    assert(scale(10.0) === 10)
    assert(scale(20.0) === 20)
    assert(scale(30.0) === 30)
    assert(scale(40.0) === 40)
    assert(scale(50.0) === 50)
    assert(scale(60.0) === 60)
    assert(scale(70.0) === 70)
    assert(scale(80.0) === 80)
    assert(scale(90.0) === 90)
    assert(scale(100.0) === 100)
  }

  test("ylinear_l1_u2_h300") {
    val scale = Scales.yscale(Scales.linear)(1.0, 2.0, 0, 300)
    assert(scale(1.0) === 300)
    assert(scale(2.0) === 0)
  }

  test("logarithmic") {
    val scale = Scales.logarithmic(0.0, 100.0, 0, 100)
    assert(scale(0.0) === 0)
    assert(scale(10.0) === 51)
    assert(scale(20.0) === 65)
    assert(scale(30.0) === 74)
    assert(scale(40.0) === 80)
    assert(scale(50.0) === 85)
    assert(scale(60.0) === 89)
    assert(scale(70.0) === 92)
    assert(scale(80.0) === 95)
    assert(scale(90.0) === 97)
    assert(scale(100.0) === 100)
  }

  test("logarithmic negative") {
    val scale = Scales.logarithmic(-100.0, 0.0, 0, 100)
    assert(scale(0.0) === 100)
    assert(scale(-10.0) === 48)
    assert(scale(-20.0) === 34)
    assert(scale(-30.0) === 25)
    assert(scale(-40.0) === 19)
    assert(scale(-50.0) === 14)
    assert(scale(-60.0) === 10)
    assert(scale(-70.0) === 7)
    assert(scale(-80.0) === 4)
    assert(scale(-90.0) === 2)
    assert(scale(-100.0) === 0)
  }

  test("logarithmic positive and negative") {
    val scale = Scales.logarithmic(-100.0, 100.0, 0, 100)
    assert(scale(100.0) === 100)
    assert(scale(50.0) === 92)
    assert(scale(10.0) === 75)
    assert(scale(0.0) === 50)
    assert(scale(-10.0) === 24)
    assert(scale(-50.0) === 7)
    assert(scale(-100.0) === 0)
  }

  test("logarithmic less than lower bound") {
    val scale = Scales.logarithmic(15.0, 100.0, 0, 100)
    assert(scale(0.0) === -150)
    assert(scale(10.0) === -20)
    assert(scale(20.0) === 14)
    assert(scale(30.0) === 35)
    assert(scale(40.0) === 51)
    assert(scale(50.0) === 62)
    assert(scale(60.0) === 72)
    assert(scale(70.0) === 80)
    assert(scale(80.0) === 88)
    assert(scale(90.0) === 94)
    assert(scale(100.0) === 100)
  }

}
