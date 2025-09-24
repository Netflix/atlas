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
  * Sum the values across the evaluation context. This is typically used to approximate
  * the distinct number of events that occurred. If the input is non-negative, then each
  * datapoint for the output line will represent the area under the input line from the
  * start of the graph to the time for that datapoint.
  */
case class OnlineIntegral(private var value: Double) extends OnlineAlgorithm {

  override def next(v: Double): Double = {
    value = Math.addNaN(value, v)
    value
  }

  override def reset(): Unit = {
    value = Double.NaN
  }

  override def isEmpty: Boolean = false

  override def state: AlgoState = {
    AlgoState("integral", "value" -> value)
  }
}

object OnlineIntegral {

  def apply(state: AlgoState): OnlineIntegral = {
    apply(state.getDouble("value"))
  }
}
