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

import scala.collection.immutable.ArraySeq

class LogLinearSuite extends FunSuite {

  private val expectedBuckets = {
    val builder = ArraySeq.newBuilder[Double]
    var v = 1L
    var delta = 1L
    while (v < 9_000_000_000_000_000_000L) {
      builder += v / 1e9
      v += delta
      if (v % (10 * delta) == 0)
        delta *= 10
    }
    builder.result()
  }

  test("bucket") {
    var i = 0
    while (i < expectedBuckets.length) {
      val v = expectedBuckets(i)
      assertEqualsDouble(LogLinear.bucket(i), v, 1e-12)
      i += 1
    }
  }

  test("bucketIndex") {
    var i = 0
    while (i < expectedBuckets.length) {
      val v = expectedBuckets(i)
      assertEquals(LogLinear.bucketIndex(v), i, s"$v")
      val v2 = v - v / 20.0
      assertEquals(LogLinear.bucketIndex(v2), i, s"$v2")
      i += 1
    }
  }

  test("bucketIndex near boundary") {
    var i = 0
    while (i < expectedBuckets.length) {
      val b = expectedBuckets(i)
      val delta = math.pow(10, math.log10(b) - 3.0)
      val v = b + delta
      assertEquals(LogLinear.bucketIndex(v), i + 1, s"$v")
      val v2 = b - delta
      assertEquals(LogLinear.bucketIndex(v2), i, s"$v2")
      i += 1
    }
  }

  test("bucketIndex near boundary negative") {
    var i = 0
    while (i < expectedBuckets.length) {
      val b = -expectedBuckets(i)
      val delta = math.pow(10, math.log10(-b) - 3.0)
      val v = b + delta
      assertEquals(LogLinear.bucketIndex(v), -i - 1, s"$v")
      val v2 = b - delta
      assertEquals(LogLinear.bucketIndex(v2), -i - 2, s"$v2")
      i += 1
    }
  }
}
