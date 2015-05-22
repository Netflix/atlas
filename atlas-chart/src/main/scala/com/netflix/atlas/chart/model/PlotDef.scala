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
package com.netflix.atlas.chart.model

import java.awt.Color

/**
 * Definition for a plot, i.e., a y-axis and associated data elements.
 *
 * @param data
 *     List of data items to include in the plot.
 * @param ylabel
 *     Label to show for the axis.
 * @param axisColor
 *     Color to use when rendering the axis.
 * @param scale
 *     Type of scale to use on the axis, linear or logarithmic.
 * @param upper
 *     Upper limit for the axis.
 * @param lower
 *     Lower limit for the axis.
 */
case class PlotDef(
    data: List[DataDef],
    ylabel: Option[String] = None,
    axisColor: Option[Color] = None,
    scale: Scale = Scale.LINEAR,
    upper: Option[Double] = None,
    lower: Option[Double] = None) {

  import java.lang.{ Double => JDouble }

  def bounds(start: Long, end: Long): (Double, Double) = {

    val dataLines = lines
    if (dataLines.isEmpty) (0.0 -> 1.0) else {
      val step = dataLines.head.data.data.step
      val (regular, stacked) = dataLines
        .filter(_.lineStyle != LineStyle.VSPAN)
        .partition(_.lineStyle != LineStyle.STACK)

      var max = -JDouble.MAX_VALUE
      var min = JDouble.MAX_VALUE
      var posSum = 0.0
      var negSum = 0.0

      var t = start
      while (t < end) {
        regular.foreach { line =>
          val v = line.data.data(t)
          max = if (v > max) v else max
          min = if (v < min) v else min
        }

        stacked.foreach { line =>
          val v = line.data.data(t)
          if (!JDouble.isNaN(v)) {
            if (v >= 0.0) posSum += v else negSum += v
          }
        }

        if (stacked.nonEmpty) {
          max = if (posSum > max) posSum else max
          min = if (negSum < min) negSum else min
        }

        posSum = 0.0
        negSum = 0.0
        t += step
      }

      val lowerBound = lower.getOrElse(min)
      val upperBound = upper.getOrElse(max)
      val bounds = (lowerBound, upperBound) match {
        case (l, u) if l > u  => 0.0 -> 1.0
        case (l, u) if l == u => (l - 1) -> (u + 1)
        case (l, u)           => l -> u
      }

      // If an area or stack is shown it will fill to zero and the filled area should be shown
      val hasArea = dataLines.exists { line =>
        line.lineStyle == LineStyle.AREA || line.lineStyle == LineStyle.STACK
      }
      if (!hasArea) bounds else {
        if (bounds._1 > 0.0)
          0.0 -> bounds._2
        else if (bounds._2 < 0.0)
          bounds._1 -> 0.0
        else
          bounds
      }
    }
  }

  def getAxisColor: Color = {
    axisColor.getOrElse(data.headOption.fold(Color.BLACK)(_.color))
  }

  def horizontalSpans: List[HSpanDef] = data.collect { case v: HSpanDef => v }

  def verticalSpans: List[VSpanDef] = data.collect { case v: VSpanDef => v }

  def lines: List[LineDef] = data.collect { case v: LineDef => v }
}

