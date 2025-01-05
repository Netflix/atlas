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
package com.netflix.atlas.core.norm

import com.netflix.atlas.core.util.Math

/**
  * Normalizes values by truncating the timestamp to the previous step boundary. All values will
  * be passed through to the `next` function.
  *
  * @param step
  *     Normalized distance between samples produced by this class.
  * @param next
  *     Normalized values will be passed to the this function.
  */
class MaxValueFunction(step: Long, next: ValueFunction) extends ValueFunction {

  private val impl = new RollingValueFunction(step, Math.maxNaN, next)

  def apply(timestamp: Long, value: Double): Unit = {
    impl.apply(timestamp, value)
  }

  override def close(): Unit = {
    impl.close()
  }

  override def toString: String = {
    s"${getClass.getSimpleName}(step=$step)"
  }
}
