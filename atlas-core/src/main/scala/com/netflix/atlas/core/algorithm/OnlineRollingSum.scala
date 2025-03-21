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
  * Sum of the values within a moving window of the input.
  *
  * @param buf
  *     Rolling buffer to keep track of the input for a given window.
  */
case class OnlineRollingSum(buf: RollingBuffer) extends OnlineAlgorithm {

  private val buffer = new RollingSumBuffer(buf)

  override def next(v: Double): Double = {
    buffer.update(v)
    if (buffer.count > 0) buffer.sum else Double.NaN
  }

  override def reset(): Unit = {
    buffer.reset()
  }

  override def isEmpty: Boolean = buf.isEmpty

  override def state: AlgoState = {
    AlgoState(
      "rolling-sum",
      "buffer" -> buf.state
    )
  }
}

object OnlineRollingSum {

  def apply(n: Int): OnlineRollingSum = apply(RollingBuffer(n))

  def apply(state: AlgoState): OnlineRollingSum = {
    apply(RollingBuffer(state.getState("buffer")))
  }
}
