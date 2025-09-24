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
package com.netflix.atlas.core.model

import munit.FunSuite

class SummaryStatsSuite extends FunSuite {

  private val start = 0L
  private val step = 60000L
  private val end = start + 2 * step

  def ts(values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, values.toArray)
    TimeSeries(Map("name" -> "test"), seq)
  }

  test("constant") {
    val stats = SummaryStats(ts(1.0, 1.0), start, end)
    assertEquals(stats.count, 2)
    assertEquals(stats.min, 1.0)
    assertEquals(stats.max, 1.0)
    assertEquals(stats.last, 1.0)
    assertEquals(stats.total, 2.0)
  }

  test("varied") {
    val stats = SummaryStats(ts(1.0, 2.0), start, end)
    assertEquals(stats.count, 2)
    assertEquals(stats.min, 1.0)
    assertEquals(stats.max, 2.0)
    assertEquals(stats.last, 2.0)
    assertEquals(stats.total, 3.0)
  }

  test("negative") {
    val stats = SummaryStats(ts(-1.0, -2.0), start, end)
    assertEquals(stats.count, 2)
    assertEquals(stats.min, -2.0)
    assertEquals(stats.max, -1.0)
    assertEquals(stats.last, -2.0)
    assertEquals(stats.total, -3.0)
  }

  test("NaN first") {
    val stats = SummaryStats(ts(Double.NaN, -2.0), start, end)
    assertEquals(stats.count, 1)
    assertEquals(stats.min, -2.0)
    assertEquals(stats.max, -2.0)
    assertEquals(stats.last, -2.0)
    assertEquals(stats.total, -2.0)
  }

  test("NaN last") {
    val stats = SummaryStats(ts(-1.0, Double.NaN), start, end)
    assertEquals(stats.count, 1)
    assertEquals(stats.min, -1.0)
    assertEquals(stats.max, -1.0)
    assertEquals(stats.last, -1.0)
    assertEquals(stats.total, -1.0)
  }

  test("Infinity") {
    val stats = SummaryStats(ts(Double.PositiveInfinity, Double.PositiveInfinity), start, end)
    assertEquals(stats.count, 2)
    assertEquals(stats.min, Double.PositiveInfinity)
    assertEquals(stats.max, Double.PositiveInfinity)
    assertEquals(stats.last, Double.PositiveInfinity)
    assertEquals(stats.total, Double.PositiveInfinity)
  }

  test("constant, Infinity") {
    val stats = SummaryStats(ts(1.0, Double.PositiveInfinity), start, end)
    assertEquals(stats.count, 2)
    assertEquals(stats.min, 1.0)
    assertEquals(stats.max, Double.PositiveInfinity)
    assertEquals(stats.last, Double.PositiveInfinity)
    assertEquals(stats.total, Double.PositiveInfinity)
  }

  test("Infinity, constant") {
    val stats = SummaryStats(ts(Double.PositiveInfinity, 1.0), start, end)
    assertEquals(stats.count, 2)
    assertEquals(stats.min, 1.0)
    assertEquals(stats.max, Double.PositiveInfinity)
    assertEquals(stats.last, 1.0)
    assertEquals(stats.total, Double.PositiveInfinity)
  }

}
