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
package com.netflix.atlas.core.algorithm

import com.netflix.atlas.core.util.Math

/**
  * Sum of the values within a moving window of the input.
  *
  * @param buf
  *     Rolling buffer to keep track of the input for a given window.
  */
case class OnlineRollingSum(buf: RollingBuffer) extends OnlineAlgorithm {

  import java.lang.{Double => JDouble}

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

    if (count > 0) sum else Double.NaN
  }

  override def reset(): Unit = {
    buf.clear()
    sum = 0.0
    count = 0
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
