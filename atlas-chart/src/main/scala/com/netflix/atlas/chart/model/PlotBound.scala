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
package com.netflix.atlas.chart.model

/**
  * Upper or lower bound to use for an axis.
  */
sealed trait PlotBound {

  def lower(hasArea: Boolean, min: Double): Double

  def upper(hasArea: Boolean, max: Double): Double
}

object PlotBound {

  import java.lang.Double as JDouble

  /**
    * Create a bound from a string representation. Acceptable values are: `auto-style`,
    * `auto-data`, or a floating point number.
    */
  def apply(s: String): PlotBound = s match {
    case "auto-style" => AutoStyle
    case "auto-data"  => AutoData
    case n            => Explicit(JDouble.parseDouble(n))
  }

  /**
    * Automatically set the bounds so the visual settings for the lines. In particular, if a line
    * style is set to area or stack, then the bounds will be adjusted so it goes to the axis.
    */
  case object AutoStyle extends PlotBound {

    def lower(hasArea: Boolean, min: Double): Double = {
      if (hasArea && min > 0.0) 0.0 else min
    }

    def upper(hasArea: Boolean, max: Double): Double = {
      if (hasArea && max < 0.0) 0.0 else max
    }

    override def toString: String = "auto-style"
  }

  /**
    * Automatically set the bounds using the min and max values for the lines.
    */
  case object AutoData extends PlotBound {

    def lower(hasArea: Boolean, min: Double): Double = min

    def upper(hasArea: Boolean, max: Double): Double = max

    override def toString: String = "auto-data"
  }

  /**
    * Sets the bound to the specified value regardless of the data in the chart.
    */
  case class Explicit(v: Double) extends PlotBound {

    def lower(hasArea: Boolean, min: Double): Double = v

    def upper(hasArea: Boolean, max: Double): Double = v

    override def toString: String = v.toString
  }

}
