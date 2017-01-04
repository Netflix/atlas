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
 * Helper to compute DES value iteratively for a set of numbers.
 *
 * @param training
 *     Number of samples to record before emitting predicted values.
 * @param alpha
 *     Data smoothing factor.
 * @param beta
 *     Trend smoothing factor.
 */
class OnlineDes(training: Int, alpha: Double, beta: Double) {
  import OnlineDes._

  private var currentSample = 0
  private var sp = Double.NaN
  private var bp = Double.NaN

  def next(v: Double): Double = {
    val retval = if (currentSample >= training) sp else Double.NaN
    val yn = v
    if (!yn.isNaN) {
      if (currentSample == 0) {
        sp = yn; bp = 0.0
      } else {
        val sn = alpha * yn + (1 - alpha) * (sp + bp)
        val bn = beta * (sn - sp) + (1 - beta) * bp
        sp = sn; bp = bn
      }
      currentSample += 1
    }
    retval
  }

  def reset(): Unit = {
    currentSample = 0
    sp = Double.NaN
    bp = Double.NaN
  }

  def state: State = State(training, alpha, beta, currentSample, sp, bp)
}

object OnlineDes {
  case class State(training: Int, alpha: Double, beta: Double, currentSample: Int, sp: Double, bp: Double)

  def apply(state: State) : OnlineDes = {
    var des = new OnlineDes(state.training, state.alpha, state.beta)
    des.currentSample = state.currentSample
    des.sp = state.sp
    des.bp = state.bp
    des
  }

  def apply(training: Int, alpha: Double, beta: Double): OnlineDes = new OnlineDes(training, alpha, beta)
}
