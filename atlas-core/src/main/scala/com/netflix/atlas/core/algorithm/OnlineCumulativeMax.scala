/*
 * Copyright 2014-2026 Netflix, Inc.
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
  * Compute the maximum value across the evaluation context. Each datapoint for the output
  * line represents the maximum value seen on the input line from the start of the graph up
  * to and including the time for that datapoint. This is the max analogue of
  * [[OnlineIntegral]]. Missing values, `NaN`, are ignored and leave the running max
  * unchanged.
  */
case class OnlineCumulativeMax(private var value: Double) extends OnlineAlgorithm {

  override def next(v: Double): Double = {
    value = Math.maxNaN(value, v)
    value
  }

  override def reset(): Unit = {
    value = Double.NaN
  }

  override def isEmpty: Boolean = false

  override def state: AlgoState = {
    AlgoState("cumulative-max", "value" -> value)
  }
}

object OnlineCumulativeMax {

  def apply(state: AlgoState): OnlineCumulativeMax = {
    apply(state.getDouble("value"))
  }
}
