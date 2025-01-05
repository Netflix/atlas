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
case class OnlineTrend(buf: RollingBuffer) extends OnlineAlgorithm {

  import java.lang.Double as JDouble

  private[this] val n = buf.values.length

  private var sum = Math.addNaN(0.0, buf.sum)
  private var nanCount = buf.values.count(JDouble.isNaN)
  private var currentSample = 0

  override def next(v: Double): Double = {
    currentSample += 1

    val removed = buf.add(v)

    if (JDouble.isNaN(removed))
      nanCount -= 1
    else
      sum -= removed

    if (JDouble.isNaN(v))
      nanCount += 1
    else
      sum += v

    if (currentSample < n || nanCount == n) Double.NaN else sum / n
  }

  override def reset(): Unit = {
    buf.clear()
    currentSample = 0
    sum = Double.NaN
    nanCount = 0
  }

  override def isEmpty: Boolean = buf.isEmpty

  override def state: AlgoState = {
    AlgoState(
      "trend",
      "buffer"        -> buf.state,
      "currentSample" -> currentSample
    )
  }
}

object OnlineTrend {

  def apply(n: Int): OnlineTrend = apply(RollingBuffer(n))

  def apply(state: AlgoState): OnlineTrend = {
    val trend = apply(RollingBuffer(state.getState("buffer")))
    trend.currentSample = state.getInt("currentSample")
    trend
  }
}
