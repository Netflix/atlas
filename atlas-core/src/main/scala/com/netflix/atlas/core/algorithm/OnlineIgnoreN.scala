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
  * Ignore the first N datapoints that are passed in. This is typically used to achieve an
  * initial alignment to step boundaries when using a deterministic sliding window approach
  * like SDES.
  */
case class OnlineIgnoreN(n: Int) extends OnlineAlgorithm {

  private var pos = 0

  override def next(v: Double): Double = {
    val i = pos
    pos += 1
    if (i >= n) v else Double.NaN
  }

  override def reset(): Unit = {
    pos = 0
  }

  override def isEmpty: Boolean = false

  override def state: AlgoState = {
    AlgoState("ignore", "n" -> n, "pos" -> pos)
  }
}

object OnlineIgnoreN {

  def apply(state: AlgoState): OnlineIgnoreN = {
    val algo = apply(state.getInt("n"))
    algo.pos = state.getInt("pos")
    algo
  }
}
