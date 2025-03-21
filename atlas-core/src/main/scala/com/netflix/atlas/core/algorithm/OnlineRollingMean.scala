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

  require(minNumValues > 0, "minimum number of values must be >= 1")
  require(buf.values.length >= minNumValues, "minimum number of values must be <= window size")

  private val buffer = new RollingSumBuffer(buf)

  override def next(v: Double): Double = {
    buffer.update(v)
    if (buffer.count >= minNumValues) buffer.sum / buffer.count else Double.NaN
  }

  override def reset(): Unit = {
    buffer.reset()
  }

  override def isEmpty: Boolean = buf.isEmpty

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
