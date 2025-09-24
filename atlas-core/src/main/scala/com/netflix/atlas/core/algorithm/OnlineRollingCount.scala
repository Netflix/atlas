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

import com.netflix.atlas.core.util.Math

/**
  * Keeps track of the number of true values within a given window. A value is considered
  * as true if it is not near zero. This is frequently used to check for a number of
  * occurrences before triggering an alert.
  */
case class OnlineRollingCount(buf: RollingBuffer) extends OnlineAlgorithm {

  private var count = buf.count

  override def next(v: Double): Double = {
    val removed = buf.add(v)
    count += Math.toBooleanDouble(v)
    count -= Math.toBooleanDouble(removed)
    count
  }

  override def reset(): Unit = {
    buf.clear()
    count = 0.0
  }

  override def isEmpty: Boolean = buf.isEmpty

  override def state: AlgoState = {
    AlgoState("rolling-count", "buffer" -> buf.state)
  }
}

object OnlineRollingCount {

  def apply(n: Int): OnlineRollingCount = apply(RollingBuffer(n))

  def apply(state: AlgoState): OnlineRollingCount = {
    apply(RollingBuffer(state.getState("buffer")))
  }
}
