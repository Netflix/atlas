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
  * Helper to compute DES value iteratively for a set of numbers.
  *
  * @param training
  *     Number of samples to record before emitting predicted values.
  * @param alpha
  *     Data smoothing factor.
  * @param beta
  *     Trend smoothing factor.
  */
case class OnlineDes(training: Int, alpha: Double, beta: Double) extends OnlineAlgorithm {

  private var currentSample = 0
  private var missingSamples = 0
  private var sp = Double.NaN
  private var bp = Double.NaN

  override def next(v: Double): Double = {
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
      missingSamples = 0
    } else {
      missingSamples += 1
    }
    retval
  }

  override def reset(): Unit = {
    currentSample = 0
    missingSamples = 0
    sp = Double.NaN
    bp = Double.NaN
  }

  override def isEmpty: Boolean = missingSamples >= training

  override def state: AlgoState = {
    AlgoState(
      "des",
      "type"           -> "des",
      "training"       -> training,
      "alpha"          -> alpha,
      "beta"           -> beta,
      "currentSample"  -> currentSample,
      "missingSamples" -> missingSamples,
      "sp"             -> sp,
      "bp"             -> bp
    )
  }
}

object OnlineDes {

  def apply(state: AlgoState): OnlineDes = {
    val des =
      new OnlineDes(state.getInt("training"), state.getDouble("alpha"), state.getDouble("beta"))
    des.currentSample = state.getInt("currentSample")
    des.missingSamples = state.getInt("missingSamples")
    des.sp = state.getDouble("sp")
    des.bp = state.getDouble("bp")
    des
  }
}
