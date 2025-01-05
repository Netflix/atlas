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

/**
  * Helper functions for log-linear scale. This scale does powers of 10 with a linear scale
  * between each power of 10. This helps emphasize some of the smaller values when there is
  * a larger overall range.
  */
private[graphics] object LogLinear {

  /**
    * Find the max value for a given bucket. If `i` is negative it will select the negative
    * bucket value.
    */
  def bucket(i: Int): Double = {
    if (i < 0)
      -bucket(-i - 1)
    else
      bucketSpan(i) * (i % 9 + 1)
  }

  /**
    * Find the span for a given bucket. This should be the previous power of 10.
    */
  def bucketSpan(i: Int): Double = {
    val idx = if (i < 0) -i - 1 else i
    val exp = idx / 9 - 9
    math.pow(10, exp)
  }

  /** Power of 10 for a long value. */
  private def pow(exp: Int): Long = {
    var result = 1L
    var i = 0
    while (i < exp) {
      result *= 10
      i += 1
    }
    result
  }

  /**
    * Find the bucket index for a given value. Negative values will return a negative index
    * that reflect the positive scale.
    */
  def bucketIndex(v: Double): Int = {
    if (v < 0.0) {
      -bucketIndex(-v) - 1
    } else if (v == 0.0) {
      0
    } else {
      val lg = math.max(-9.0, math.floor(math.log10(v)))
      val prevBuckets = (lg.toInt + 9) * 9
      val E = 6.0 - lg
      if (E >= 0.0) {
        // For values in this range convert to Long and use integer math
        // to avoid some problems with floating point
        val n = (v * math.pow(10, E)).toLong
        val exp = lg.toInt + E.toInt
        val p10 = pow(exp)
        ((n - 1) / p10).toInt + prevBuckets
      } else {
        val p10 = math.pow(10, lg)
        val delta = v - p10
        math.ceil(delta / p10).toInt + prevBuckets
      }
    }
  }

  private def ratio(v: Double, i: Int): Double = {
    if (v < 0.0) {
      1.0 - ratio(-v, -i - 1)
    } else {
      val span = bucketSpan(i)
      val boundary = bucket(i) - span
      (v - boundary) / span
    }
  }

  /** Determine the pixel position for a given value. */
  def position(v: Double, min: Int, pixelsPerBucket: Double): Double = {
    val i = bucketIndex(v)
    val offset = math.max(0.0, i - min - 1) * pixelsPerBucket
    ratio(v, i) * pixelsPerBucket + offset
  }
}
