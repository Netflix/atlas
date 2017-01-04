/*
 * Copyright 2014-2017 Netflix, Inc.
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
class OnlineSlidingDes(training: Int, alpha: Double, beta: Double, des1: OnlineDes, des2: OnlineDes) {
  import OnlineSlidingDes._

  private var useOne = true
  private var currentSample = 0

  def next(v: Double): Double = {
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

  def reset(): Unit = {
    des1.reset()
    des2.reset()
  }

  def state: State = State(training, alpha, beta, useOne, currentSample, des1.state, des2.state)
}

object OnlineSlidingDes {
  case class State(training: Int, alpha: Double, beta: Double, useOne: Boolean, currentSample: Int,
                   des1State: OnlineDes.State, des2State: OnlineDes.State)

  def apply(state: OnlineSlidingDes.State): OnlineSlidingDes = {
    val des1 = OnlineDes(state.des1State)
    val des2 = OnlineDes(state.des2State)
    var sdes = new OnlineSlidingDes(state.training, state.alpha, state.beta, des1, des2)
    sdes.useOne = state.useOne
    sdes.currentSample = state.currentSample
    sdes
  }

  def apply (training: Int, alpha: Double, beta: Double): OnlineSlidingDes = {
    val des1 = OnlineDes(training, alpha, beta)
    val des2 = OnlineDes(training, alpha, beta)
    new OnlineSlidingDes(training, alpha, beta, des1, des2)
  }
}
