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
  * Value function that will aggregate all values received for a given step interval. Keeps
  * a rolling buffer to allow out of order updates for a brief window and ensure that the
  * normalized values will get output in order.
  *
  * @param step
  *     Normalized distance between samples produced by this class.
  * @param aggr
  *     Aggregation function to use to combine values for the same interval.
  * @param next
  *     Normalized values will be passed to the this function.
  */
class RollingValueFunction(
  step: Long,
  aggr: (Double, Double) => Double,
  next: ValueFunction
) extends ValueFunction {

  require(step >= 1, "step must be >= 1")

  private[this] val size = 2

  private val values = Array.fill[Double](size)(Double.NaN)
  private var lastUpdateTime = -1L

  /**
    * Truncate the timestamp to the step boundary and pass the value to the next function if the
    * actual timestamp on the measurement is newer than the last timestamp seen by this function.
    */
  private def normalize(timestamp: Long): Long = {
    val stepBoundary = timestamp / step * step
    if (timestamp == stepBoundary)
      stepBoundary
    else
      stepBoundary + step
  }

  override def apply(timestamp: Long, value: Double): Unit = {
    val t = normalize(timestamp) / step
    val delta = if (lastUpdateTime < 0) 1 else t - lastUpdateTime

    if (delta == 0 || (delta < 0 && -delta < size)) {
      // Update the current entry or old entry that is still within range
      val i = (t % size).toInt
      values(i) = aggr(values(i), value)
      writeValue(t, values(i))
    } else if (delta >= 1) {
      // Create or overwrite an older entry
      val i = (t % size).toInt
      lastUpdateTime = t
      values(i) = value
      writeValue(t, values(i))
    }
  }

  override def close(): Unit = {
    lastUpdateTime = -1L
    next.close()
  }

  private def writeValue(timestamp: Long, value: Double): Unit = {
    if (!value.isNaN) {
      next(timestamp * step, value)
    }
  }

  override def toString: String = {
    s"${getClass.getSimpleName}(step=$step)"
  }
}
