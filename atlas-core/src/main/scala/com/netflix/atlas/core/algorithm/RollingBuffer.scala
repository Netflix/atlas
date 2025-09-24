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
package com.netflix.atlas.core.algorithm

import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.Math

/**
  * Buffer for tracking the last N values of a time series.
  *
  * @param values
  *     Underlying array that is used to store the values. As new data is added it will roll
  *     through the array and overwrite old values that are now out of the window. The window
  *     size is the size of the array.
  * @param start
  *     Starting position within the array. This is typically only used when restoring from
  *     state captured from another buffer.
  */
class RollingBuffer(val values: Array[Double], start: Int = 0) {

  require(values.length > 0, "values array cannot be empty")
  require(start >= 0, s"starting position is out of bounds: $start < 0")
  require(start < values.length, s"starting position is out of bounds: $start >= ${values.length}")

  private[this] var pos = start

  /** Returns the number of non-NaN values that are currently in the buffer. */
  var size: Int = values.count(!_.isNaN)

  private def next(): Unit = {
    pos = (pos + 1) % values.length
  }

  def add(v: Double): Double = {
    val previous = values(pos)
    values(pos) = v
    next()
    if (!v.isNaN) size += 1
    if (!previous.isNaN) size -= 1
    previous
  }

  def count: Double = {
    var result = 0.0
    var i = 0
    while (i < values.length) {
      result += Math.toBooleanDouble(values(i))
      i += 1
    }
    result
  }

  def sum: Double = {
    var result = Double.NaN
    var i = 0
    while (i < values.length) {
      result = Math.addNaN(result, values(i))
      i += 1
    }
    result
  }

  def min: Double = {
    var result = Double.NaN
    var i = 0
    while (i < values.length) {
      result = Math.minNaN(result, values(i))
      i += 1
    }
    result
  }

  def max: Double = {
    var result = Double.NaN
    var i = 0
    while (i < values.length) {
      result = Math.maxNaN(result, values(i))
      i += 1
    }
    result
  }

  def clear(): Unit = {
    var i = 0
    while (i < values.length) {
      values(i) = Double.NaN
      i += 1
    }
    pos = 0
    size = 0
  }

  def isEmpty: Boolean = size == 0

  def state: AlgoState = {
    val vs = java.util.Arrays.copyOf(values, values.length)
    AlgoState("rolling-buffer", "values" -> vs, "pos" -> pos)
  }

  override def toString: String = {
    s"RollingBuffer([${values.mkString(",")}], start=$start, pos=$pos)"
  }
}

object RollingBuffer {

  /** Create a new buffer of size `n` initialized with NaN values. */
  def apply(n: Int): RollingBuffer = {
    new RollingBuffer(ArrayHelper.fill(n, Double.NaN))
  }

  /** Create a new buffer based on previously captured state. */
  def apply(state: AlgoState): RollingBuffer = {
    val values = state.getDoubleArray("values")
    val pos = state.getInt("pos")
    new RollingBuffer(values, pos)
  }
}
