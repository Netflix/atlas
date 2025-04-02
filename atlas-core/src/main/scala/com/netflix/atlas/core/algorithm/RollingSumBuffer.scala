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
  * Buffer for OnlineRollingSum and OnlineRollingMean.
  */
private[algorithm] class RollingSumBuffer(val buf: RollingBuffer) {

  import java.lang.Double as JDouble

  var sum: Double = Math.addNaN(0.0, buf.sum)
  var count: Int = buf.values.count(v => !JDouble.isNaN(v))

  def update(v: Double): Unit = {
    val removed = buf.add(v)

    val isRemovedNaN = JDouble.isNaN(removed)
    val isValueNaN = JDouble.isNaN(v)
    if (!isRemovedNaN && !isValueNaN) {
      adjustSum(-removed + v)
    } else if (!isRemovedNaN) {
      adjustSum(-removed)
      count -= 1
    } else if (!isValueNaN) {
      adjustSum(v)
      count += 1
    }
  }

  /**
    * Check if there is a large discrepancy between value sizes and either adjust the sum
    * using the delta or recompute from the buffer.
    */
  private def adjustSum(delta: Double): Unit = {
    val newSum = Math.addNaN(sum, delta)
    val expSum = getExponent(sum)
    val expNewSum = getExponent(newSum)
    if (java.lang.Math.abs(expSum - expNewSum) < 15)
      sum = newSum
    else
      sum = buf.sum
  }

  /**
    * Get the exponent for a double value, special case 0 to return 0 instead of -1023.
    */
  private def getExponent(v: Double): Int = {
    val exp = java.lang.Math.getExponent(v)
    if (exp == -1023) 0 else exp
  }

  def reset(): Unit = {
    buf.clear()
    sum = 0.0
    count = 0
  }
}
