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
package com.netflix.atlas.core.algorithm

/**
  * Keeps track of the minimum value within a given window. This is typically used as a
  * way to get a smooth lower bound line that closely tracks a noisy input.
  */
case class OnlineRollingMin(buf: RollingBuffer) extends OnlineAlgorithm {
  private var min = buf.min

  override def next(v: Double): Double = {
    val removed = buf.add(v)
    if (v <= min) { // Avoid doing linear computation of min value if possible
      min = v
    } else if (removed == min || (buf.size > 0 && min.isNaN)) {
      min = buf.min
    }
    min
  }

  override def reset(): Unit = {
    buf.clear()
    min = Double.NaN
  }

  override def isEmpty: Boolean = buf.isEmpty

  override def state: AlgoState = {
    AlgoState("rolling-min", "buffer" -> buf.state)
  }
}

object OnlineRollingMin {

  def apply(n: Int): OnlineRollingMin = apply(RollingBuffer(n))

  def apply(state: AlgoState): OnlineRollingMin = {
    apply(RollingBuffer(state.getState("buffer")))
  }
}
