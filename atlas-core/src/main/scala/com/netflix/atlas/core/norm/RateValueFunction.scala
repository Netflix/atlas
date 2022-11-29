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

/**
  * Converts values into a rate per second. The input values should be a monotonically increasing.
  */
class RateValueFunction(next: ValueFunction) extends ValueFunction {

  private var lastUpdateTime: Long = -1L
  private var lastValue: Double = 0.0

  def apply(timestamp: Long, value: Double): Unit = {
    if (lastUpdateTime > 0L) {
      val duration = (timestamp - lastUpdateTime) / 1000.0
      val delta = value - lastValue
      val rate = if (duration <= 0.0 || delta <= 0.0) 0.0 else delta / duration
      next(timestamp, rate)
    }
    lastUpdateTime = timestamp
    lastValue = value
  }

  override def close(): Unit = {
    next.close()
  }
}
