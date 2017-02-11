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

import scala.util.Random


class TicksSuite extends FunSuite {

  private def checkForDuplicates(ticks: List[ValueTick]): Unit = {
    val duplicates = ticks.filter(_.major).map(_.label).groupBy(v => v).filter(_._2.size > 1)
    assert(duplicates === Map.empty, "duplicate tick labels")
  }

  private def sanityCheck(ticks: List[ValueTick]): Unit = {
    checkForDuplicates(ticks)
  }

  test("values [0.0, 100.0]") {
    val ticks = Ticks.value(0.0, 100.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 6)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "100.0")
  }

  test("values [1.0, 2.0], 7 ticks") {
    val ticks = Ticks.value(1.0, 2.0, 7)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 6)
  }

  test("values [0.0, 10.0]") {
    val ticks = Ticks.value(0.0, 10.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 6)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "10.0")
  }

  test("values [0.0, 8.0]") {
    val ticks = Ticks.value(0.0, 8.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 17)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "8.0")
  }

  test("values [0.0, 7.0]") {
    val ticks = Ticks.value(0.0, 7.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 15)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.filter(_.major).map(_.label).mkString(",") === "0.0,2.0,4.0,6.0")
  }

  test("values [0.96, 1.0]") {
    val ticks = Ticks.value(0.96, 1.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.96")
    assert(ticks.last.label === "1.00")
  }

  test("values [835, 1068]") {
    val ticks = Ticks.value(835.0, 1068, 5)
    sanityCheck(ticks)
    assert(ticks.size === 23)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    //assert(ticks.filter(_.major).head.label === "0.85k")
    assert(ticks.filter(_.major).last.label === "1.05k")
  }

  test("values [2026, 2027]") {
    val ticks = Ticks.value(2026.0, 2027.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 6)
    assert(ticks.head.offset === 2026.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "1.0")
  }

  test("values [200026, 200027]") {
    val ticks = Ticks.value(200026.0, 200027.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 6)
    assert(ticks.head.offset === 200026.0)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "1.0")
  }

  test("values [200026.23, 200026.2371654]") {
    val ticks = Ticks.value(200026.23, 200026.2371654, 5)
    sanityCheck(ticks)
    assert(ticks.size === 15)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 200026.23)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "7.0m")
  }

  test("values [2026, 2047]") {
    val ticks = Ticks.value(2026.0, 2047.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 22)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.filter(_.major).head.label === "2.030k")
    assert(ticks.filter(_.major).last.label === "2.045k")
  }

  test("values [20, 21.8]") {
    val ticks = Ticks.value(20.0, 21.8, 5)
    sanityCheck(ticks)
    assert(ticks.size === 19)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "20.0")
    assert(ticks.last.label === "21.8")
  }

  test("values [-21.8, -20]") {
    val ticks = Ticks.value(-21.8, -20.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 19)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "-21.8")
    assert(ticks.last.label === "-20.0")
  }

  test("values [-2027, -2046]") {
    val ticks = Ticks.value(-2027.0, -2026.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 6)
    assert(ticks.head.offset === -2027.0)
    assert(ticks.filter(_.major).head.label === "0.0")
    assert(ticks.filter(_.major).last.label === "1.0")
  }

  test("values [42.0, 8.123456e12]") {
    val ticks = Ticks.value(42.0, 8.123456e12, 5)
    sanityCheck(ticks)
    assert(ticks.size === 16)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0.5T")
    assert(ticks.last.label === "8.0T")
  }

  test("values [2126.4044472658984, 2128.626188548245], 9 ticks") {
    val ticks = Ticks.value(2126.4044472658984, 2128.626188548245, 9)
    sanityCheck(ticks)
    assert(ticks.size === 22)
    assert(ticks.count(_.major) === 7)
    assert(ticks.head.offset === 2126.4)
    assert(ticks.head.label === "100.0m")
    assert(ticks.last.label === "2.2")
  }

  test("values [0.0, 1.468m]") {
    val ticks = Ticks.value(0.0, 1.468e-3, 5)
    sanityCheck(ticks)
    assert(ticks.size === 15)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.filter(_.major).head.label === "0.0m")
    assert(ticks.filter(_.major).last.label === "1.2m")
  }

  // TODO: major tick labels should be possible for this range without using an offset.
  test("values [4560.0, 4569.9]") {
    val ticks = Ticks.value(4559.9, 4569.9, 5)
    sanityCheck(ticks)
    assert(ticks.size === 20)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 4558.0)
    assert(ticks.head.label === "2.0")
    assert(ticks.last.label === "11.5")
  }

  test("values [0.0, Infinity]") {
    val t = intercept[IllegalArgumentException] {
      Ticks.value(0.0, Double.PositiveInfinity, 5)
    }
    assert(t.getMessage === "requirement failed: upper bound must be finite")
  }

  test("values [-Infinity, 0.0]") {
    val t = intercept[IllegalArgumentException] {
      Ticks.value(Double.NegativeInfinity, 0.0, 5)
    }
    assert(t.getMessage === "requirement failed: lower bound must be finite")
  }

  test("values [0.0, MaxValue]") {
    val ticks = Ticks.value(0.0, Double.MaxValue, 5)
  }

  test("sanity check, 0 to y") {
    for (i <- 0 until 100; j <- 2 until 10) {
      val v = Random.nextDouble() * 1e12
      try {
        val ticks = Ticks.value(0.0, v, j)
        sanityCheck(ticks)
      } catch {
        case t: Throwable =>
          throw new AssertionError(s"Ticks.value(0.0, $v, $j)", t)
      }
    }
  }

  test("sanity check, y1 to y2") {
    for (i <- 0 until 100; j <- 2 until 10) {
      val v1 = Random.nextDouble() * 1e4
      val v2 = v1 + Random.nextDouble() * 1e3
      try {
        val ticks = Ticks.value(v1, v2, j)
        sanityCheck(ticks)
      } catch {
        case t: Throwable =>
          throw new AssertionError(s"Ticks.value($v1, $v2, $j)", t)
      }
    }
  }

  test("binary [0, 1024]") {
    val ticks = Ticks.binary(0.0, 1024, 5)
    sanityCheck(ticks)
    assert(ticks.size === 11)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0")
    assert(ticks.last.label === "1000")
  }

  test("binary [0, 1258]") {
    val ticks = Ticks.binary(0.0, 1258, 5)
    sanityCheck(ticks)
    assert(ticks.size === 13)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0")
    assert(ticks.last.label === "1200")
  }

  test("binary [0, 7258]") {
    val ticks = Ticks.binary(0.0, 7258, 5)
    sanityCheck(ticks)
    assert(ticks.size === 15)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0")
    assert(ticks.last.label === "7168")
  }

  test("binary [0, 10k]") {
    val ticks = Ticks.binary(0.0, 10e3, 5)
    sanityCheck(ticks)
    assert(ticks.size === 20)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "0Ki")
    assert(ticks.last.label === "10Ki")
  }

  test("binary [8k, 10k]") {
    val ticks = Ticks.binary(8e3, 10e3, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 6)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "7.8Ki")
    assert(ticks.last.label === "9.8Ki")
  }

  test("binary [8k, 10M]") {
    val ticks = Ticks.binary(8e3, 10e6, 5)
    sanityCheck(ticks)
    assert(ticks.size === 19)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "512Ki")
    assert(ticks.last.label === "9728Ki")
  }

  test("binary [8k, 10T]") {
    val ticks = Ticks.binary(8e3, 10e12, 5)
    sanityCheck(ticks)
    assert(ticks.size === 18)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 0.0)
    assert(ticks.head.label === "512Gi")
    assert(ticks.last.label === "9216Gi")
  }

  test("binary [9993, 10079]") {
    val ticks = Ticks.binary(9993, 10079, 5)
    sanityCheck(ticks)
    assert(ticks.size === 17)
    assert(ticks.count(_.major) === 4)
    assert(ticks.head.offset === 9980.0)
    assert(ticks.head.label === "15")
    assert(ticks.last.label === "95")
  }

  test("binary [9991234563, 9991234567]") {
    val ticks = Ticks.binary(9991234563.0, 9991234567.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 21)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 9.991234563E9)
    assert(ticks.head.label === "0")
    assert(ticks.last.label === "4")
  }

  test("binary [99912000000, 9991800000]") {
    val ticks = Ticks.binary(99912000000.0, 99918000000.0, 5)
    sanityCheck(ticks)
    assert(ticks.size === 11)
    assert(ticks.count(_.major) === 3)
    assert(ticks.head.offset === 9.9910418432E10)
    assert(ticks.head.label === "2048Ki")
    assert(ticks.last.label === "7168Ki")
  }

  test("binary [339.86152734763687, 339.87716933873867]") {
    val ticks =Ticks.binary(339.86152734763687, 339.87716933873867, 5)
    sanityCheck(ticks)
    assert(ticks.size === 2)
    assert(ticks.count(_.major) === 2)
    assert(ticks.head.offset === 339.86152734763687)
    assert(ticks.head.label === "0.0")
    assert(ticks.last.label === "15.6m")
  }

  test("binary [1e12, 1e12 + 5e6]") {
    val ticks =Ticks.binary(1e12, 1e12 + 5e6, 5)
    sanityCheck(ticks)
    assert(ticks.size === 19)
    assert(ticks.count(_.major) === 5)
    assert(ticks.head.offset === 9.99999668224E11)
    assert(ticks.head.label === "512Ki")
    assert(ticks.last.label === "5120Ki")
  }

  test("binary sanity check, 0 to y") {
    for (i <- 0 until 100; j <- 2 until 10) {
      val v = Random.nextDouble() * 1e12
      try {
        val ticks = Ticks.binary(0.0, v, j)
        sanityCheck(ticks)
      } catch {
        case t: Throwable =>
          throw new AssertionError(s"Ticks.binary(0.0, $v, $j)", t)
      }
    }
  }

  test("binary sanity check, y1 to y2") {
    for (i <- 0 until 100; j <- 2 until 10) {
      val v1 = Random.nextDouble() * 1e4
      val v2 = v1 + Random.nextDouble() * 1e3
      try {
        val ticks = Ticks.binary(v1, v2, j)
        sanityCheck(ticks)
      } catch {
        case t: Throwable =>
          throw new AssertionError(s"Ticks.binary($v1, $v2, $j)", t)
      }
    }
  }
}
