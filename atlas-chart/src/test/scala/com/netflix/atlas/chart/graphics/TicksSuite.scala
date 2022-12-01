/*
 * Copyright 2014-2022 Netflix, Inc.
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

import java.time.ZoneOffset

import munit.FunSuite

import scala.util.Random

class TicksSuite extends FunSuite {

  private def checkForDuplicates(ticks: List[ValueTick]): Unit = {
    val duplicates = ticks.filter(_.major).map(_.label).groupBy(v => v).filter(_._2.size > 1)
    assertEquals(duplicates, Map.empty[String, List[String]], "duplicate tick labels")
  }

  private def sanityCheck(ticks: List[ValueTick]): Unit = {
    checkForDuplicates(ticks)
  }

  test("values [0.0, 100.0]") {
    val ticks = Ticks.value(0.0, 100.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0.0")
    assertEquals(ticks.last.label, "100.0")
  }

  test("values [1.0, 2.0], 7 ticks") {
    val ticks = Ticks.value(1.0, 2.0, 7)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
  }

  test("values [0.0, 10.0]") {
    val ticks = Ticks.value(0.0, 10.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0.0")
    assertEquals(ticks.last.label, "10.0")
  }

  test("values [0.0, 8.0]") {
    val ticks = Ticks.value(0.0, 8.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 17)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0.0")
    assertEquals(ticks.last.label, "8.0")
  }

  test("values [0.0, 7.0]") {
    val ticks = Ticks.value(0.0, 7.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 15)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.filter(_.major).map(_.label).mkString(","), "0.0,2.0,4.0,6.0")
  }

  test("values [0.96, 1.0]") {
    val ticks = Ticks.value(0.96, 1.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0.96")
    assertEquals(ticks.last.label, "1.00")
  }

  test("values [835, 1068]") {
    val ticks = Ticks.value(835.0, 1068, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 23)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    // assertEquals(ticks.filter(_.major).head.label, "0.85k")
    assertEquals(ticks.filter(_.major).last.label, "1.05k")
  }

  test("values [2026, 2027]") {
    val ticks = Ticks.value(2026.0, 2027.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 2026.0)
    assertEquals(ticks.head.label, "0.0")
    assertEquals(ticks.last.label, "1.0")
  }

  test("values [200026, 200027]") {
    val ticks = Ticks.value(200026.0, 200027.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 200026.0)
    assertEquals(ticks.head.label, "0.0")
    assertEquals(ticks.last.label, "1.0")
  }

  test("values [200026.23, 200026.2371654]") {
    val ticks = Ticks.value(200026.23, 200026.2371654, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 15)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 200026.23)
    assertEquals(ticks.head.label, "0.0")
    assertEquals(ticks.last.label, "7.0m")
  }

  test("values [2026, 2047]") {
    val ticks = Ticks.value(2026.0, 2047.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 22)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.filter(_.major).head.label, "2.030k")
    assertEquals(ticks.filter(_.major).last.label, "2.045k")
  }

  test("values [20, 21.8]") {
    val ticks = Ticks.value(20.0, 21.8, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "20.0")
    assertEquals(ticks.last.label, "21.8")
  }

  test("values [-21.8, -20]") {
    val ticks = Ticks.value(-21.8, -20.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "-21.8")
    assertEquals(ticks.last.label, "-20.0")
  }

  test("values [-2027, -2046]") {
    val ticks = Ticks.value(-2027.0, -2026.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, -2027.0)
    assertEquals(ticks.filter(_.major).head.label, "0.0")
    assertEquals(ticks.filter(_.major).last.label, "1.0")
  }

  test("values [42.0, 8.123456e12]") {
    val ticks = Ticks.value(42.0, 8.123456e12, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 16)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0.5T")
    assertEquals(ticks.last.label, "8.0T")
  }

  test("values [2126.4044472658984, 2128.626188548245], 9 ticks") {
    val ticks = Ticks.value(2126.4044472658984, 2128.626188548245, 9)
    sanityCheck(ticks)
    assertEquals(ticks.size, 22)
    assertEquals(ticks.count(_.major), 7)
    assertEquals(ticks.head.offset, 2126.4)
    assertEquals(ticks.head.label, "100.0m")
    assertEquals(ticks.last.label, "2.2")
  }

  test("values [0.0, 1.468m]") {
    val ticks = Ticks.value(0.0, 1.468e-3, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 15)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.filter(_.major).head.label, "0.0m")
    assertEquals(ticks.filter(_.major).last.label, "1.2m")
  }

  test("values [4560.0, 4569.9]") {
    val ticks = Ticks.value(4559.9, 4569.9, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "4.560k")
    assertEquals(ticks.last.label, "4.570k")
  }

  test("values [0.0, Infinity]") {
    val t = intercept[IllegalArgumentException] {
      Ticks.value(0.0, Double.PositiveInfinity, 5)
    }
    assertEquals(t.getMessage, "requirement failed: upper bound must be finite")
  }

  test("values [-Infinity, 0.0]") {
    val t = intercept[IllegalArgumentException] {
      Ticks.value(Double.NegativeInfinity, 0.0, 5)
    }
    assertEquals(t.getMessage, "requirement failed: lower bound must be finite")
  }

  test("values [0.0, MaxValue]") {
    Ticks.value(0.0, Double.MaxValue, 5)
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
    assertEquals(ticks.size, 11)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0")
    assertEquals(ticks.last.label, "1000")
  }

  test("binary [0, 1258]") {
    val ticks = Ticks.binary(0.0, 1258, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 13)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0")
    assertEquals(ticks.last.label, "1200")
  }

  test("binary [0, 7258]") {
    val ticks = Ticks.binary(0.0, 7258, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 15)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0")
    assertEquals(ticks.last.label, "7168")
  }

  test("binary [0, 10k]") {
    val ticks = Ticks.binary(0.0, 10e3, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0Ki")
    assertEquals(ticks.last.label, "10Ki")
  }

  test("binary [8k, 10k]") {
    val ticks = Ticks.binary(8e3, 10e3, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "7.8Ki")
    assertEquals(ticks.last.label, "9.8Ki")
  }

  test("binary [8k, 10M]") {
    val ticks = Ticks.binary(8e3, 10e6, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "512Ki")
    assertEquals(ticks.last.label, "9728Ki")
  }

  test("binary [8k, 10T]") {
    val ticks = Ticks.binary(8e3, 10e12, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 18)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "512Gi")
    assertEquals(ticks.last.label, "9216Gi")
  }

  test("binary [9993, 10079]") {
    val ticks = Ticks.binary(9993, 10079, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 17)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 9980.0)
    assertEquals(ticks.head.label, "15")
    assertEquals(ticks.last.label, "95")
  }

  test("binary [9991234563, 9991234567]") {
    val ticks = Ticks.binary(9991234563.0, 9991234567.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 9.991234563e9)
    assertEquals(ticks.head.label, "0")
    assertEquals(ticks.last.label, "4")
  }

  test("binary [99912000000, 9991800000]") {
    val ticks = Ticks.binary(99912000000.0, 99918000000.0, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 11)
    assertEquals(ticks.count(_.major), 3)
    assertEquals(ticks.head.offset, 9.9910418432e10)
    assertEquals(ticks.head.label, "2048Ki")
    assertEquals(ticks.last.label, "7168Ki")
  }

  test("binary [339.86152734763687, 339.87716933873867]") {
    val ticks = Ticks.binary(339.86152734763687, 339.87716933873867, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 2)
    assertEquals(ticks.count(_.major), 2)
    assertEquals(ticks.head.offset, 339.86152734763687)
    assertEquals(ticks.head.label, "0.0")
    assertEquals(ticks.last.label, "15.6m")
  }

  test("binary [1e12, 1e12 + 5e6]") {
    val ticks = Ticks.binary(1e12, 1e12 + 5e6, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 9.99999668224e11)
    assertEquals(ticks.head.label, "512Ki")
    assertEquals(ticks.last.label, "5120Ki")
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

  test("duration [0, 59]") {
    val ticks = Ticks.duration(0.0, 59, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0.0s")
    assertEquals(ticks.last.label, "57.0s")
  }

  test("duration [0, 60]") {
    val ticks = Ticks.duration(0.0, 60, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0s")
    assertEquals(ticks.last.label, "60s")
  }

  test("duration [0, 3659]") {
    val ticks = Ticks.duration(0.0, 3659, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0m")
    assertEquals(ticks.last.label, "60m")
  }

  test("duration [0, 3600]") {
    val ticks = Ticks.duration(0.0, 3659, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0m")
    assertEquals(ticks.last.label, "60m")
  }

  test("duration [0, 86400]") {
    val ticks = Ticks.duration(0.0, 86400, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 25)
    assertEquals(ticks.count(_.major), 7)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0h")
    assertEquals(ticks.last.label, "24h")
  }

  test("duration [86400, 86500]") {
    val ticks = Ticks.duration(0.0, 86400, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 25)
    assertEquals(ticks.count(_.major), 7)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0h")
    assertEquals(ticks.last.label, "24h")
  }

  test("duration [86400, 87500]") {
    val ticks = Ticks.duration(0.0, 86400, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 25)
    assertEquals(ticks.count(_.major), 7)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0h")
    assertEquals(ticks.last.label, "24h")
  }

  test("duration [0, 2592000]") {
    val ticks = Ticks.duration(0.0, 2592000, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 18)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0w")
    assertEquals(ticks.last.label, "4w")
  }

  test("duration [0, 31536000]") {
    val ticks = Ticks.duration(0.0, 31536000, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 17)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0w")
    assertEquals(ticks.last.label, "52w")
  }

  test("duration [1.0, 2.0], 7 ticks") {
    val ticks = Ticks.duration(1.0, 2.0, 7)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
  }

  test("duration [339.86152734763687, 339.87716933873867]") {
    val ticks = Ticks.duration(339.86152734763687, 339.87716933873867, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 16)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 339.86)
    assertEquals(ticks.head.label, "2.000ms")
    assertEquals(ticks.last.label, "17.000ms")
  }

  test("duration [59, 61]") {
    val ticks = Ticks.duration(59, 61, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 58.800000000000004)
    assertEquals(ticks.head.label, "0.2s")
    assertEquals(ticks.last.label, "2.1s")
  }

  test("duration [59, 3605]") {
    val ticks = Ticks.duration(59, 3605, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "3m")
    assertEquals(ticks.last.label, "60m")
  }

  test("duration [0, 3153600000]") {
    val ticks = Ticks.duration(0.0, 3153600000L, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0y")
    assertEquals(ticks.last.label, "100y")
  }

  test("duration [3153600000, 3153600005]") {
    val ticks = Ticks.duration(3153600000L, 3153600005L, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 3.1536e9)
    assertEquals(ticks.head.label, "0.0s")
    assertEquals(ticks.last.label, "5.0s")
  }

  test("duration [0.01, 1]") {
    val ticks = Ticks.duration(0.01, 1, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0.1s")
    assertEquals(ticks.last.label, "1.0s")
  }

  test("duration [0.01, 0.1]") {
    val ticks = Ticks.duration(0.01, 0.1, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "10.0ms")
    assertEquals(ticks.last.label, "100.0ms")
  }

  test("duration [0.0001, 0.1]") {
    val ticks = Ticks.duration(0.0001, 0.1, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "5.0ms")
    assertEquals(ticks.last.label, "100.0ms")
  }

  test("duration [0.0001, 0.001]") {
    val ticks = Ticks.duration(0.0001, 0.001, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "100.0μs")
    assertEquals(ticks.last.label, "1000.0μs")
  }

  test("duration [0.0000001, 0.001]") {
    val ticks = Ticks.duration(0.0000001, 0.001, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "50.0μs")
    assertEquals(ticks.last.label, "1000.0μs")
  }

  test("duration [45, 300]") {
    val ticks = Ticks.duration(45, 300, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 18)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "1m")
    assertEquals(ticks.last.label, "5m")
  }

  test("duration [45, 300e3]") {
    val ticks = Ticks.duration(45, 300e3, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 13)
    assertEquals(ticks.count(_.major), 3)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "0d")
    assertEquals(ticks.last.label, "3d")
  }

  test("duration [45, 300e5]") {
    val ticks = Ticks.duration(45, 300e5, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 15)
    assertEquals(ticks.count(_.major), 3)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "3w")
    assertEquals(ticks.last.label, "49w")
  }

  test("duration [45, 300e8]") {
    val ticks = Ticks.duration(45, 300e8, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 4)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "50y")
    assertEquals(ticks.last.label, "950y")
  }

  // TODO goofy edge case due to ieee 754 causes 3 decimals. Fix it some day if
  // we want.
  test("duration [0.0000001, 0.000001]") {
    val ticks = Ticks.duration(0.0000001, 0.000001, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 19)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "100.000ns")
    assertEquals(ticks.last.label, "1000.000ns")
  }

  test("duration [1.0e-10, 1.0e-9]") {
    val ticks = Ticks.duration(1.0e-10, 1.0e-9, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 18)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "150.0ps")
    assertEquals(ticks.last.label, "1000.0ps")
  }

  test("duration [1.0e-13, 1.0e-12]") {
    val ticks = Ticks.duration(1.0e-13, 1.0e-12, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 0)
  }

  test("duration [1.0e-12, 1.0e-9]") {
    val ticks = Ticks.duration(1.0e-12, 1.0e-9, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "50.0ps")
    assertEquals(ticks.last.label, "1000.0ps")
  }

  test("duration [1.0e-12, 1.0e12]") {
    val ticks = Ticks.duration(1.0e-12, 1.0e12, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 20)
    assertEquals(ticks.count(_.major), 5)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "1.6e+03y")
    assertEquals(ticks.last.label, "3.2e+04y")
  }

  test("duration [-15, -1]") {
    val ticks = Ticks.duration(-15, -1, 5)
    sanityCheck(ticks)
    assertEquals(ticks.size, 15)
    assertEquals(ticks.count(_.major), 3)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "-15.0s")
    assertEquals(ticks.last.label, "-1.0s")
  }

  test("time: since 1970") {
    val s = 0L
    val e = 1498751868000L
    val ticks = Ticks.time(s, e, ZoneOffset.UTC, 5)
    assertEquals(ticks.size, 6)
  }

  test("issue-948: [6.667e-3, 0.01]") {
    val ticks = Ticks.value(6.667e-3, 0.01, 7)
    sanityCheck(ticks)
    assertEquals(ticks.size, 34)
    assertEquals(ticks.count(_.major), 7)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "6.7m")
    assertEquals(ticks.last.label, "10.0m")
  }

  test("issue-991: [99.938845, 100]") {
    val ticks = Ticks.value(99.938845, 100, 7)
    sanityCheck(ticks)
    assertEquals(ticks.size, 31)
    assertEquals(ticks.count(_.major), 7)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "99.94")
    assertEquals(ticks.last.label, "100.00")
  }

  test("issue-991: [99.9, 100]") {
    val ticks = Ticks.value(99.9, 100, 7)
    sanityCheck(ticks)
    assertEquals(ticks.size, 21)
    assertEquals(ticks.count(_.major), 6)
    assertEquals(ticks.head.offset, 0.0)
    assertEquals(ticks.head.label, "99.90")
    assertEquals(ticks.last.label, "100.00")
  }

  // Debug helper.
  /*private def tickPrint(ticks: List[ValueTick]): Unit = {
    System.out.println("------------------")
    ticks.reverse.foreach(t =>
      if (t.major) {
        System.out.println(s"${t.label} *  [${t.offset}] off")
      } else {
        System.out.println(s"${t.label} -  [${t.offset}] off")
      }
    )
    System.out.println("-------------------")
  }*/
}
