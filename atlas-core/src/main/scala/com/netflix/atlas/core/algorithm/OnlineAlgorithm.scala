/*
 * Copyright 2014-2019 Netflix, Inc.
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
  * Base trait for online algorithms used on time series.
  */
trait OnlineAlgorithm {

  /** Apply the next value from the input and return the computed value. */
  def next(v: Double): Double

  /** Reset the state of the algorithm. */
  def reset(): Unit

  /**
    * Capture the current state of the algorithm. It can be restored in a new instance
    * with the [OnlineAlgorithm#apply] method.
    */
  def state: AlgoState
}

object OnlineAlgorithm {

  /**
    * Create a new instance initialized with the captured state from a previous instance
    * of an online algorithm.
    */
  def apply(state: AlgoState): OnlineAlgorithm = {
    state.algorithm match {
      case "delay"       => OnlineDelay(state)
      case "des"         => OnlineDes(state)
      case "ignore"      => OnlineIgnoreN(state)
      case "pipeline"    => Pipeline(state)
      case "rolling-min" => OnlineRollingMin(state)
      case "rolling-max" => OnlineRollingMax(state)
      case "sliding-des" => OnlineSlidingDes(state)
      case t             => throw new IllegalArgumentException(s"unknown type: '$t'")
    }
  }
}
