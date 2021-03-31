/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.core.norm

import com.netflix.atlas.core.util.Math

/**
  * Buffer used to track the last `n` values over time for a given normalization
  * function. This is typically to allow for use-cases where data may be reported
  * on many nodes and thus some data is received out of order.
  *
  * @param step
  *     Step size between successive values.
  * @param size
  *     Size of the buffer.
  */
class RollingValueBuffer(step: Long, size: Int) {

  private val values = Array.fill[Double](size)(Double.NaN)
  private var lastUpdateTime = -1L

  private def index(timestamp: Long): Int = {
    val t = timestamp / step
    val minTime = lastUpdateTime - step * size
    if (t <= minTime) {
      -1
    } else if (t > lastUpdateTime) {
      lastUpdateTime = t
      val i = (t % size).toInt
      values(i) = Double.NaN
      i
    } else {
      (t % size).toInt
    }
  }

  def set(timestamp: Long, value: Double): Double = {
    val i = index(timestamp)
    if (i >= 0) {
      values(i) = value
      value
    } else {
      Double.NaN
    }
  }

  def add(timestamp: Long, value: Double): Double = {
    val i = index(timestamp)
    if (i >= 0) {
      values(i) = Math.addNaN(values(i), value)
      values(i)
    } else {
      Double.NaN
    }
  }

  def max(timestamp: Long, value: Double): Double = {
    val i = index(timestamp)
    if (i >= 0) {
      values(i) = Math.maxNaN(values(i), value)
      values(i)
    } else {
      Double.NaN
    }
  }
}
