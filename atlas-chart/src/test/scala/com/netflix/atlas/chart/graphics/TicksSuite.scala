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
package com.netflix.atlas.chart.graphics

import org.scalatest.FunSuite


class TicksSuite extends FunSuite {

  test("values [0.0, 100.0]") {
    val ticks = Ticks.value(0.0, 100.0, 5)
    assert(ticks.size === 21)
    assert(ticks.filter(_.major).size === 6)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "100.0")
  }

  test("values [0.0, 10.0]") {
    val ticks = Ticks.value(0.0, 10.0, 5)
    assert(ticks.size === 21)
    assert(ticks.filter(_.major).size === 6)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "10.0")
  }

  test("values [0.0, 8.0]") {
    val ticks = Ticks.value(0.0, 8.0, 5)
    assert(ticks.size === 17)
    assert(ticks.filter(_.major).size === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "8.0")
  }

  test("values [0.0, 7.0]") {
    val ticks = Ticks.value(0.0, 7.0, 5)
    assert(ticks.size === 15)
    assert(ticks.filter(_.major).size === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.filter(_.major).map(_.label).mkString(",") === "0.0,1.5,3.0,4.5,6.0")
  }

  test("values [2026, 2027]") {
    val ticks = Ticks.value(2026.0, 2027.0, 5)
    assert(ticks.size === 21)
    assert(ticks.filter(_.major).size === 6)
    assert(ticks.head.offset === 2026.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "1.0")
  }

  test("values [200026, 200027]") {
    val ticks = Ticks.value(200026.0, 200027.0, 5)
    assert(ticks.size === 21)
    assert(ticks.filter(_.major).size === 6)
    assert(ticks.head.offset === 200026.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "1.0")
  }

  test("values [200026.23, 200026.2371654]") {
    val ticks = Ticks.value(200026.23, 200026.2371654, 5)
    assert(ticks.size === 15)
    assert(ticks.filter(_.major).size === 5)
    assert(ticks.head.offset === 200026.23)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "7.0m")
  }

  test("values [2026, 2047]") {
    val ticks = Ticks.value(2026.0, 2047.0, 5)
    assert(ticks.size === 22)
    assert(ticks.filter(_.major).size === 4)
    assert(ticks.head.offset === 2026.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "21.0")
  }

  test("values [20, 21.8]") {
    val ticks = Ticks.value(20.0, 21.8, 5)
    assert(ticks.size === 19)
    assert(ticks.filter(_.major).size === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "20.0")
    assert(ticks.last.label === "21.8")
  }

  test("values [-21.8, -20]") {
    val ticks = Ticks.value(-21.8, -20.0, 5)
    assert(ticks.size === 19)
    assert(ticks.filter(_.major).size === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "-21.8")
    assert(ticks.last.label === "-20.0")
  }

  test("values [-2027, -2046]") {
    val ticks = Ticks.value(-2027.0, -2026.0, 5)
    assert(ticks.size === 21)
    assert(ticks.filter(_.major).size === 6)
    assert(ticks.head.offset === -2027.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "1.0")
  }

  test("values [42.0, 8.123456e12]") {
    val ticks = Ticks.value(42.0, 8.123456e12, 5)
    assert(ticks.size === 16)
    assert(ticks.filter(_.major).size === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "500.0G")
    assert(ticks.last.label === "8.0T")
  }
}
