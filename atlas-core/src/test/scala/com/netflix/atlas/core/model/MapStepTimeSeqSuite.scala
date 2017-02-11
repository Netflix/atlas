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
package com.netflix.atlas.core.model

import org.scalatest.FunSuite

class MapStepTimeSeqSuite extends FunSuite {

  import ConsolidationFunction._

  private val start = 60L

  def gauge(start: Long, step: Long, values: Double*): TimeSeq = {
    new ArrayTimeSeq(DsType.Gauge, start, step, values.toArray)
  }

  def rate(start: Long, step: Long, values: Double*): TimeSeq = {
    new ArrayTimeSeq(DsType.Rate, start, step, values.toArray)
  }

  def map(ds: DsType, s1: Long, s2: Long, cf: ConsolidationFunction, values: Double*): TimeSeq = {
    val mStart = start / s2 * s2
    val multiple = s2 / s1
    val boundaries =
      (if (start != mStart) 1 else 0) +
      (if (multiple > 0 && values.length % multiple != 0) 1 else 0)
    val timeRange = if (multiple <= 0) s1 * values.length else {
      s2 * (values.length / multiple + boundaries)
    }
    val mEnd = mStart + timeRange
    val seq = if (ds == DsType.Rate) rate(start, s1, values: _*) else gauge(start, s1, values: _*)
    new MapStepTimeSeq(seq, s2, cf).bounded(mStart, mEnd)
  }

  test("consolidate: bad step") {
    intercept[IllegalArgumentException] {
      map(DsType.Gauge, 2, 7, Sum, 1.0)
    }
  }

  test("consolidate: sum") {
    assert(map(DsType.Gauge, 1, 2, Sum, 1.0, 2.0) === gauge(start, 2, 3.0))
  }

  test("consolidate: sum with partial interval") {
    assert(map(DsType.Gauge, 1, 2, Sum, 1.0, 2.0, 3.0) === gauge(start, 2, 3.0, 3.0))
  }

  test("consolidate: sum with start time not on step boundary") {
    val step = 7
    val cstart = start / step * step
    val expected = gauge(cstart, step, 6.0, 22.0)
    assert(map(DsType.Gauge, 1, step, Sum, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0) === expected)
  }

  test("consolidate: sum with NaN") {
    assert(map(DsType.Gauge, 1, 2, Sum, 1.0, Double.NaN) === gauge(start, 2, 1.0))
  }

  test("consolidate: sum with all NaN") {
    assert(map(DsType.Gauge, 1, 2, Sum, Double.NaN, Double.NaN) === gauge(start, 2, Double.NaN))
  }

  test("consolidate: avg") {
    assert(map(DsType.Gauge, 1, 2, Avg, 1.0, 2.0) === gauge(start, 2, 1.5))
  }

  test("consolidate: avg with NaN") {
    assert(map(DsType.Gauge, 1, 2, Avg, 1.0, Double.NaN) === gauge(start, 2, 1.0))
  }

  test("consolidate: avg rate with NaN") {
    assert(map(DsType.Rate, 1, 2, Avg, 1.0, Double.NaN) === rate(start, 2, 0.5))
  }

  test("consolidate: avg with all NaN") {
    assert(map(DsType.Gauge, 1, 2, Avg, Double.NaN, Double.NaN) === gauge(start, 2, Double.NaN))
  }

  test("consolidate: min") {
    assert(map(DsType.Gauge, 1, 2, Min, 1.0, 2.0) === gauge(start, 2, 1.0))
  }

  test("consolidate: min with NaN") {
    assert(map(DsType.Gauge, 1, 2, Min, 1.0, Double.NaN) === gauge(start, 2, 1.0))
  }

  test("consolidate: min with all NaN") {
    assert(map(DsType.Gauge, 1, 2, Min, Double.NaN, Double.NaN) === gauge(start, 2, Double.NaN))
  }

  test("consolidate: max") {
    assert(map(DsType.Gauge, 1, 2, Max, 1.0, 2.0) === gauge(start, 2, 2.0))
  }

  test("consolidate: max with NaN") {
    assert(map(DsType.Gauge, 1, 2, Max, 1.0, Double.NaN) === gauge(start, 2, 1.0))
  }

  test("consolidate: max with all NaN") {
    assert(map(DsType.Gauge, 1, 2, Max, Double.NaN, Double.NaN) === gauge(start, 2, Double.NaN))
  }

  test("expand: sum") {
    assert(map(DsType.Gauge, 2, 1, Sum, 1.0, 2.0) === gauge(start, 1, 1.0, 1.0, 2.0, 2.0))
  }

  test("expand: bad step") {
    intercept[IllegalArgumentException] {
      map(DsType.Gauge, 4, 3, Sum, 1.0)
    }
  }
}

