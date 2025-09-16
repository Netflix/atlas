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

import com.netflix.atlas.core.util.Step
import munit.FunSuite

class MapStepTimeSeqSuite extends FunSuite {

  import ConsolidationFunction.*

  private val start = 61L

  private def gauge(start: Long, step: Long, values: Double*): ArrayTimeSeq = {
    new ArrayTimeSeq(DsType.Gauge, start, step, values.toArray)
  }

  private def rate(start: Long, step: Long, values: Double*): ArrayTimeSeq = {
    new ArrayTimeSeq(DsType.Rate, start, step, values.toArray)
  }

  def map(ds: DsType, s1: Long, s2: Long, cf: ConsolidationFunction, values: Double*): TimeSeq = {
    val s = Step.roundToStepBoundary(start, s1)
    val mStart = Step.roundToStepBoundary(s, s2)
    val seq = if (ds == DsType.Rate) rate(s, s1, values*) else gauge(s, s1, values*)
    val e = Step.roundToStepBoundary(seq.end, s2)
    val mEnd = if (e % s2 == 0) e + s2 else e
    new MapStepTimeSeq(seq, s2, cf).bounded(mStart, mEnd)
  }

  test("consolidate: bad step") {
    intercept[IllegalArgumentException] {
      map(DsType.Gauge, 2, 7, Sum, 1.0)
    }
  }

  test("consolidate: sum") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Sum, 1.0, 2.0), gauge(s, 2, 3.0, Double.NaN))
  }

  test("consolidate: sum with partial intervals") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Sum, 1.0, 2.0, 3.0), gauge(s, 2, 3.0, 3.0))
  }

  test("consolidate: sum with start time not on step boundary") {
    val step = 7
    val cstart = start / step * step + step
    val expected = gauge(cstart, step, 6.0, 22.0)
    assertEquals(map(DsType.Gauge, 1, step, Sum, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0), expected)
  }

  test("consolidate: sum with NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Sum, 1.0, Double.NaN), gauge(s, 2, 1.0, Double.NaN))
  }

  test("consolidate: sum with all NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(
      map(DsType.Gauge, 1, 2, Sum, Double.NaN, Double.NaN),
      gauge(s, 2, Double.NaN, Double.NaN)
    )
  }

  test("consolidate: avg") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Avg, 1.0, 2.0), gauge(s, 2, 1.5, Double.NaN))
  }

  test("consolidate: avg with NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Avg, 1.0, Double.NaN), gauge(s, 2, 1.0, Double.NaN))
  }

  test("consolidate: avg rate with NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Rate, 1, 2, Avg, 1.0, Double.NaN), rate(s, 2, 0.5, Double.NaN))
  }

  test("consolidate: avg with all NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(
      map(DsType.Gauge, 1, 2, Avg, Double.NaN, Double.NaN),
      gauge(s, 2, Double.NaN, Double.NaN)
    )
  }

  test("consolidate: min") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Min, 1.0, 2.0), gauge(s, 2, 1.0, Double.NaN))
  }

  test("consolidate: min with NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Min, 1.0, Double.NaN), gauge(s, 2, 1.0, Double.NaN))
  }

  test("consolidate: min with all NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(
      map(DsType.Gauge, 1, 2, Min, Double.NaN, Double.NaN),
      gauge(s, 2, Double.NaN, Double.NaN)
    )
  }

  test("consolidate: max") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Max, 1.0, 2.0), gauge(s, 2, 2.0, Double.NaN))
  }

  test("consolidate: max with NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(map(DsType.Gauge, 1, 2, Max, 1.0, Double.NaN), gauge(s, 2, 1.0, Double.NaN))
  }

  test("consolidate: max with all NaN") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(
      map(DsType.Gauge, 1, 2, Max, Double.NaN, Double.NaN),
      gauge(s, 2, Double.NaN, Double.NaN)
    )
  }

  test("expand: sum") {
    val s = Step.roundToStepBoundary(start, 2)
    assertEquals(
      map(DsType.Gauge, 2, 1, Sum, 1.0, 2.0),
      gauge(s, 1, 1.0, 2.0, 2.0, Double.NaN, Double.NaN)
    )
  }

  test("expand: bad step") {
    intercept[IllegalArgumentException] {
      map(DsType.Gauge, 4, 3, Sum, 1.0)
    }
  }
}
