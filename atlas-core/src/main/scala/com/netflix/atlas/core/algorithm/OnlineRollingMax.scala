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
  * Keeps track of the maximum value within a given window. This is typically used as a
  * way to get a smooth upper bound line that closely tracks a noisy input.
  */
case class OnlineRollingMax(buf: RollingBuffer) extends OnlineAlgorithm {

  private var max = buf.max

  override def next(v: Double): Double = {
    val removed = buf.add(v)
    if (v >= max) { // Avoid doing linear computation of max value if possible
      max = v
    } else if (removed == max || (buf.size > 0 && max.isNaN)) {
      max = buf.max
    }
    max
  }

  override def reset(): Unit = {
    buf.clear()
    max = Double.NaN
  }

  override def isEmpty: Boolean = buf.isEmpty

  override def state: AlgoState = {
    AlgoState("rolling-max", "buffer" -> buf.state)
  }
}

object OnlineRollingMax {

  def apply(n: Int): OnlineRollingMax = apply(RollingBuffer(n))

  def apply(state: AlgoState): OnlineRollingMax = {
    apply(RollingBuffer(state.getState("buffer")))
  }
}
