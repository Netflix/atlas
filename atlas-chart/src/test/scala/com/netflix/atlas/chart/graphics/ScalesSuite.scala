/*
 * Copyright 2014-2017 Netflix, Inc.
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

import org.scalatest.FunSuite


class ScalesSuite extends FunSuite {

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
    val scale = Scales.ylinear(1.0, 2.0, 0, 300)
    assert(scale(1.0) === 300)
    assert(scale(2.0) === 0)
  }

  test("logarithmic") {
    val scale = Scales.logarithmic(0.0, 100.0, 0, 100)
    assert(scale(0.0) === 0)
    assert(scale(10.0) === 50)
    assert(scale(20.0) === 65)
    assert(scale(30.0) === 73)
    assert(scale(40.0) === 80)
    assert(scale(50.0) === 84)
    assert(scale(60.0) === 88)
    assert(scale(70.0) === 92)
    assert(scale(80.0) === 95)
    assert(scale(90.0) === 97)
    assert(scale(100.0) === 100)
  }

  test("logarithmic negative") {
    val scale = Scales.logarithmic(-100.0, 0.0, 0, 100)
    assert(scale(0.0) === 100)
    assert(scale(-10.0) === 50)
    assert(scale(-20.0) === 35)
    assert(scale(-30.0) === 27)
    assert(scale(-40.0) === 20)
    assert(scale(-50.0) === 16)
    assert(scale(-60.0) === 12)
    assert(scale(-70.0) === 8)
    assert(scale(-80.0) === 5)
    assert(scale(-90.0) === 3)
    assert(scale(-100.0) === 0)
  }

  test("logarithmic positive and negative") {
    val scale = Scales.logarithmic(-100.0, 100.0, 0, 100)
    assert(scale(100.0) === 100)
    assert(scale(50.0) === 92)
    assert(scale(10.0) === 75)
    assert(scale(0.0) === 50)
    assert(scale(-10.0) === 25)
    assert(scale(-50.0) === 8)
    assert(scale(-100.0) === 0)
  }

}
