/*
 * Copyright 2014-2019 Netflix, Inc.
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

import com.netflix.atlas.core.util.Math

/**
  * Mean of the values within a moving window of the input. The denominator is the number
  * of values (non-NaN entries) in the rolling buffer.
  *
  * @param buf
  *     Rolling buffer to keep track of the input for a given window.
  * @param minNumValues
  *     Minimum number of values that must be present within the buffer for an average
  *     to be emitted. If there are not enough non-NaN values, then NaN will be emitted.
  */
case class OnlineRollingMean(buf: RollingBuffer, minNumValues: Int) extends OnlineAlgorithm {

  import java.lang.{Double => JDouble}

  require(buf.values.length >= minNumValues, "minimum number of values must be <= window size")

  private[this] val n = buf.values.length

  private[this] var sum = Math.addNaN(0.0, buf.sum)
  private[this] var count = buf.values.count(v => !JDouble.isNaN(v))

  override def next(v: Double): Double = {
    val removed = buf.add(v)

    if (!JDouble.isNaN(removed)) {
      sum -= removed
      count -= 1
    }

    if (!JDouble.isNaN(v)) {
      sum += v
      count += 1
    }

    if (count >= minNumValues) sum / count else Double.NaN
  }

  override def reset(): Unit = {
    buf.clear()
    sum = 0.0
    count = 0
  }

  override def state: AlgoState = {
    AlgoState(
      "rolling-mean",
      "buffer"       -> buf.state,
      "minNumValues" -> minNumValues
    )
  }
}

object OnlineRollingMean {

  def apply(n: Int, minNumValues: Int): OnlineRollingMean = apply(RollingBuffer(n), minNumValues)

  def apply(state: AlgoState): OnlineRollingMean = {
    val minIntervals = state.getInt("minNumValues")
    apply(RollingBuffer(state.getState("buffer")), minIntervals)
  }
}
