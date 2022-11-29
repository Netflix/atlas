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
package com.netflix.atlas.core.norm

/**
  * If it is expensive to overwrite, then the immediate pass through behavior of
  * other functions can be undesirable. This implementation uses a buffer to track
  * the last `n` values over time for a given normalization function. This is
  * typically to allow for use-cases where data may be reported on many nodes and
  * thus some data is received out of order with many values for a given time.
  *
  * @param step
  *     Step size between successive values.
  * @param size
  *     Size of the buffer.
  * @param next
  *     Normalized values will be passed to the this function.
  */
class DedupValueFunction(step: Long, size: Int, next: ValueFunction) extends ValueFunction {

  private val times = Array.fill[Long](size)(-1L)
  private val values = Array.fill[Double](size)(Double.NaN)

  override def apply(timestamp: Long, value: Double): Unit = {
    val t = timestamp / step
    val i = (t % size).toInt
    if (timestamp == times(i)) {
      values(i) = value
    } else {
      flushTo(times(i))
      times(i) = timestamp
      values(i) = value
    }
  }

  private def flushTo(cutoff: Long): Unit = {
    var i = 0
    while (i < size) {
      if (times(i) >= 0L && times(i) <= cutoff) {
        if (!values(i).isNaN)
          next(times(i), values(i))
        times(i) = -1L
        values(i) = Double.NaN
      }
      i += 1
    }
  }

  override def close(): Unit = {
    var i = 0
    while (i < size) {
      if (times(i) >= 0L && !values(i).isNaN)
        next(times(i), values(i))
      i += 1
    }
    next.close()
  }
}
