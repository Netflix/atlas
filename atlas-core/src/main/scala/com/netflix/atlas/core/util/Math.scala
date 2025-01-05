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
package com.netflix.atlas.core.util

object Math {

  import java.lang.Double as JDouble

  /**
    * Check if a double value is nearly zero, i.e., within a small amount from 0. For our purposes
    * the small amount is `1e-12`. Not sure of the original reason for choosing that amount, but it
    * has been used for a long time.
    */
  def isNearlyZero(v: Double): Boolean = {
    JDouble.isNaN(v) || scala.math.abs(v) < 1e-12
  }

  /**
    * Convert a double value to a boolean. NaN and nearly 0 values are considered false, all other
    * values are true.
    */
  def toBoolean(v: Double): Boolean = {
    !isNearlyZero(v)
  }

  /**
    * Convert a double value to a boolean. NaN and nearly 0 values are considered false, all other
    * values are true.
    */
  def toBooleanDouble(v: Double): Double = {
    if (toBoolean(v)) 1.0 else 0.0
  }

  /** Add two double values treating NaN as 0 if one side is a number. */
  def addNaN(v1: Double, v2: Double): Double = {
    if (JDouble.isNaN(v1)) v2 else if (JDouble.isNaN(v2)) v1 else v1 + v2
  }

  /** Subtract two double values treating NaN as 0 if one side is a number. */
  def subtractNaN(v1: Double, v2: Double): Double = addNaN(v1, -v2)

  /** Find the max of two double values treating NaN as smaller if one side is a number. */
  def maxNaN(v1: Double, v2: Double): Double = {
    if (JDouble.isNaN(v1))
      v2
    else if (JDouble.isNaN(v2)) v1
    else if (v1 > v2) v1
    else v2
  }

  /** Find the min of two double values treating NaN as smaller if one side is a number. */
  def minNaN(v1: Double, v2: Double): Double = {
    if (JDouble.isNaN(v1))
      v2
    else if (JDouble.isNaN(v2)) v1
    else if (v1 < v2) v1
    else v2
  }

  /** Boolean greater than of two doubles treating NaN as smaller if one side is a number. */
  def gtNaN(v1: Double, v2: Double): Double = {
    if (JDouble.isNaN(v1) && JDouble.isNaN(v2)) Double.NaN
    else {
      val t1 = if (JDouble.isNaN(v1)) 0.0 else v1
      val t2 = if (JDouble.isNaN(v2)) 0.0 else v2
      if (t1 > t2) 1.0 else 0.0
    }
  }

  /** Boolean greater than of two doubles treating NaN as smaller if one side is a number. */
  def ltNaN(v1: Double, v2: Double): Double = {
    if (JDouble.isNaN(v1) && JDouble.isNaN(v2)) Double.NaN
    else {
      val t1 = if (JDouble.isNaN(v1)) 0.0 else v1
      val t2 = if (JDouble.isNaN(v2)) 0.0 else v2
      if (t1 < t2) 1.0 else 0.0
    }
  }
}
