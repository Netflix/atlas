/*
 * Copyright 2015 Netflix, Inc.
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

/**
 * Normalizes values by truncating the timestamp to the previous step boundary. All values will
 * be passed through to the `next` function.
 *
 * @param step
 *     Normalized distance between samples produced by this class.
 * @param next
 *     Normalized values will be passed to the this function.
 */
class LastValueFunction(step: Long, next: ValueFunction)
    extends ValueFunction {

  require(step >= 1, "step must be >= 1")

  /**
   * The last time an update was received. Used to ensure that we move forward and do not pass
   * through older measurements that arrived at this function at a later time.
   */
  private var lastUpdateTime: Long = -1L

  /**
   * Truncate the timestamp to the step boundary and pass the value to the next function if the
   * actual timestamp on the measurement is newer than the last timestamp seen by this function.
   */
  def apply(timestamp: Long, value: Double) {
    if (timestamp > lastUpdateTime) {
      lastUpdateTime = timestamp
      val stepBoundary = timestamp / step * step
      if (timestamp == stepBoundary)
        next(stepBoundary, value)
      else
        next(stepBoundary + step, value)
    }
  }

  override def toString: String = {
    s"${getClass.getSimpleName}(step=$step, lastUpdateTime=$lastUpdateTime)"
  }
}
