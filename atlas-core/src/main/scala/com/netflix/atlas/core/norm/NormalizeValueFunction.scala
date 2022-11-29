/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.core.norm

import com.netflix.spectator.api.Spectator

object NormalizeValueFunction {

  private final val NO_PREVIOUS_UPDATE = -1L

  private val heartbeatExpireCount =
    Spectator.globalRegistry().counter("atlas.norm.heartbeatExpireCount")
}

/**
  * Normalizes metrics in a manner similar to RRDtool.
  *
  * @param step        normalized distance between samples produced by this class.
  * @param heartbeat   maximum time allowed between updates before NaN values will be used.
  * @param next        normalized values will be passed to the this function.
  */
class NormalizeValueFunction(step: Long, heartbeat: Long, next: ValueFunction)
    extends ValueFunction {

  require(step >= 1, "step must be >= 1")
  require(heartbeat >= 1, "heartbeat must be >= 1")

  /**
    * The last time an update was received.
    */
  private var lastUpdateTime: Long = NormalizeValueFunction.NO_PREVIOUS_UPDATE

  /**
    * Represents the value from the beginning of the current interval to the
    * last update time.
    */
  private var lastValue: Double = 0.0

  /**
    * Update with a new sample and return the value for a complete interval if
    * one is available.
    */
  def apply(timestamp: Long, value: Double): Unit = {
    if (timestamp > lastUpdateTime) {
      if (lastUpdateTime > 0 && timestamp - lastUpdateTime > heartbeat) {
        NormalizeValueFunction.heartbeatExpireCount.increment()
        lastUpdateTime = NormalizeValueFunction.NO_PREVIOUS_UPDATE
        lastValue = 0.0
      }

      val offset = timestamp % step
      val stepBoundary = timestamp - offset

      // Check to see if we crossed a step boundary, if so then we need to report a normalized
      // value to the next value function
      if (lastUpdateTime < stepBoundary) {
        if (lastUpdateTime != NormalizeValueFunction.NO_PREVIOUS_UPDATE) {
          val intervalOffset = lastUpdateTime % step
          var nextBoundary = lastUpdateTime - intervalOffset + step

          lastValue += weightedValue(step - intervalOffset, value)
          next(nextBoundary, lastValue)

          while (nextBoundary < stepBoundary) {
            nextBoundary += step
            next(nextBoundary, value)
          }
        } else if (offset == 0) {
          next(timestamp, value)
        } else {
          next(stepBoundary, weightedValue(step - offset, value))
        }

        lastUpdateTime = timestamp
        lastValue = weightedValue(offset, value)
      } else {
        // Didn't cross step boundary, so update is more frequent than step and we just need to
        // add in the weighted value
        val intervalOffset = timestamp - lastUpdateTime
        lastUpdateTime = timestamp
        lastValue += weightedValue(intervalOffset, value)
      }
    }
  }

  override def close(): Unit = {
    next.close()
  }

  private def weightedValue(offset: Long, value: Double): Double = {
    val weight = offset.asInstanceOf[Double] / step
    value * weight
  }

  override def toString: String = {
    val name = getClass.getSimpleName
    "%s(step=%d, heartbeat=%d, lastUpdateTime=%d, lastValue=%f)".format(
      name,
      step,
      heartbeat,
      lastUpdateTime,
      lastValue
    )
  }
}
