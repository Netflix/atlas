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
  * Alternate between two DES functions after each training period. This provides a deterministic
  * estimate within a bounded amount of time.
  *
  * @param training
  *     Number of samples to record before emitting predicted values.
  * @param alpha
  *     Data smoothing factor.
  * @param beta
  *     Trend smoothing factor.
  */
case class OnlineSlidingDes(
  training: Int,
  alpha: Double,
  beta: Double,
  des1: OnlineDes,
  des2: OnlineDes
) extends OnlineAlgorithm {

  private var useOne = true
  private var currentSample = 0

  override def next(v: Double): Double = {
    currentSample += 1
    val v1 = des1.next(v)
    val v2 = des2.next(v)
    val retval = if (useOne) v1 else v2

    if (currentSample % training == 0) {
      if (useOne) des1.reset() else des2.reset()
      useOne = !useOne
    }

    retval
  }

  override def reset(): Unit = {
    des1.reset()
    des2.reset()
  }

  override def isEmpty: Boolean = false

  override def state: AlgoState = {
    AlgoState(
      "sliding-des",
      "training"      -> training,
      "alpha"         -> alpha,
      "beta"          -> beta,
      "useOne"        -> useOne,
      "currentSample" -> currentSample,
      "des1"          -> des1.state,
      "des2"          -> des2.state
    )
  }
}

object OnlineSlidingDes {

  def apply(state: AlgoState): OnlineSlidingDes = {
    val des1 = OnlineDes(state.getState("des1"))
    val des2 = OnlineDes(state.getState("des2"))
    val sdes = new OnlineSlidingDes(
      state.getInt("training"),
      state.getDouble("alpha"),
      state.getDouble("beta"),
      des1,
      des2
    )
    sdes.useOne = state.getBoolean("useOne")
    sdes.currentSample = state.getInt("currentSample")
    sdes
  }

  def apply(training: Int, alpha: Double, beta: Double): OnlineSlidingDes = {
    val des1 = OnlineDes(training, alpha, beta)
    val des2 = OnlineDes(training, alpha, beta)
    new OnlineSlidingDes(training, alpha, beta, des1, des2)
  }
}
