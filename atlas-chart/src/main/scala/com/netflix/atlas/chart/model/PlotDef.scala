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

import com.netflix.atlas.chart.graphics.Heatmap
import com.netflix.atlas.chart.graphics.LeftValueAxis
import com.netflix.atlas.chart.graphics.Style

import java.awt.Color
import com.netflix.atlas.chart.graphics.Theme
import com.netflix.atlas.chart.graphics.TimeAxis
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model.PlotBound.Explicit
import com.netflix.atlas.core.model.TimeSeq

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
  * @param tickLabelMode
  *     Mode to use for displaying tick labels.
  * @param heatmap
  *     Optional heatmap settings for the plot.
  */
case class PlotDef(
  data: List[DataDef],
  ylabel: Option[String] = None,
  axisColor: Option[Color] = None,
  scale: Scale = Scale.LINEAR,
  upper: PlotBound = AutoStyle,
  lower: PlotBound = AutoStyle,
  tickLabelMode: TickLabelMode = TickLabelMode.DECIMAL,
  heatmap: Option[HeatmapDef] = None
) {

  import java.lang.Double as JDouble

  (lower, upper) match {
    case (Explicit(l), Explicit(u)) =>
      require(l < u, s"lower bound must be less than upper bound ($l >= $u)")
    case (_, _) =>
  }

  def bounds(start: Long, end: Long): (Double, Double) = {

    val dataLines = lines
    if (dataLines.isEmpty) 0.0 -> 1.0
    else {
      val step = dataLines.head.data.data.step
      val (regular, stacked) = dataLines
        .filter(line => line.lineStyle != LineStyle.VSPAN && !Heatmap.isPercentileHeatmap(line))
        .partition(_.lineStyle != LineStyle.STACK)

      var max = -JDouble.MAX_VALUE
      var min = JDouble.MAX_VALUE
      var posSum = 0.0
      var negSum = 0.0

      dataLines
        .filter(Heatmap.isPercentileHeatmap)
        .filter(line => hasNonZeroData(start, end, step, line.data.data))
        .flatMap(line => Heatmap.percentileBucketRange(line.data.tags))
        .foreach {
          case (mn, mx) =>
            min = math.min(min, mn)
            max = math.max(max, mx)
        }

      var t = start
      while (t < end) {
        regular.foreach { line =>
          val v = line.data.data(t)
          if (JDouble.isFinite(v)) {
            max = if (v > max) v else max
            min = if (v < min) v else min
          }
        }

        stacked.foreach { line =>
          val v = line.data.data(t)
          if (JDouble.isFinite(v)) {
            if (v >= 0.0) posSum += v else negSum += v
          }
        }

        if (stacked.nonEmpty) {
          val v = stacked.head.data.data(t)
          if (JDouble.isFinite(v)) {
            max = if (v > max) v else max
            min = if (v < min) v else min
          }

          max = if (posSum > 0.0 && posSum > max) posSum else max
          min = if (negSum < 0.0 && negSum < min) negSum else min
        }

        posSum = 0.0
        negSum = 0.0
        t += step
      }

      // If an area or stack is shown it will fill to zero and the filled area should be shown
      val hasArea = dataLines.exists { line =>
        line.lineStyle == LineStyle.AREA || line.lineStyle == LineStyle.STACK
      }

      min = if (min == JDouble.MAX_VALUE) 0.0 else min
      max = if (max == -JDouble.MAX_VALUE) 1.0 else max
      finalBounds(hasArea, min, max)
    }
  }

  private def hasNonZeroData(start: Long, end: Long, step: Long, ts: TimeSeq): Boolean = {
    var t = start
    while (t < end) {
      val v = ts(t)
      if (!v.isNaN && v != 0.0)
        return true
      t += step
    }
    false
  }

  private[model] def finalBounds(hasArea: Boolean, min: Double, max: Double): (Double, Double) = {

    // Try to figure out bounds following the guidelines:
    // * An explicit bound should always get used.
    // * If an area is present, then automatic bounds should go to the 0 line.
    // * If an automatic bound equals or is on the wrong side of an explicit bound, then pad by 1.
    val l = lower.lower(hasArea, min)
    val u = upper.upper(hasArea, max)

    // If upper and lower bounds are equal or automatic/explicit combination causes lower to be
    // greater than the upper, then pad automatic bounds by 1. Explicit bounds should
    // be honored.
    if (l < u) l -> u
    else {
      (lower, upper) match {
        case (Explicit(_), Explicit(_)) => l       -> u
        case (_, Explicit(_))           => (u - 1) -> u
        case (Explicit(_), _)           => l       -> (l + 1)
        case (_, _)                     => l       -> (u + 1)
      }
    }
  }

  def getAxisColor(dflt: Color): Color = {
    axisColor.getOrElse(dflt)
  }

  def showTickLabels: Boolean = tickLabelMode != TickLabelMode.OFF

  def horizontalSpans: List[HSpanDef] = data.collect { case v: HSpanDef => v }

  def verticalSpans: List[VSpanDef] = data.collect { case v: VSpanDef => v }

  def lines: List[LineDef] = data.collect { case v: LineDef => v }

  def renderedLines: List[LineDef] = data.collect {
    case v: LineDef if v.lineStyle != LineStyle.HEATMAP => v
  }

  def heatmapLines: List[LineDef] = data.collect {
    case v: LineDef if v.lineStyle == LineStyle.HEATMAP => v
  }

  def legendData: List[DataDef] = data.filter {
    case v: LineDef if v.lineStyle == LineStyle.HEATMAP => false
    case _                                              => true
  }

  def normalize(theme: Theme): PlotDef = {
    copy(axisColor = Some(getAxisColor(theme.legend.text.color)), heatmap = heatmapSettings)
  }

  def heatmapSettings: Option[HeatmapDef] = {
    if (heatmapLines.nonEmpty) {
      val settings = heatmap.getOrElse(HeatmapDef())
      val palette = settings.palette
        .getOrElse {
          Palette.gradient(heatmapLines.head.color)
        }
        .copy(name = "heatmap")
      Some(settings.copy(palette = Some(palette)))
    } else {
      None
    }
  }

  def heatmapData(gdef: GraphDef): Option[Heatmap] = {
    if (heatmapLines.nonEmpty) {
      val settings = heatmap.getOrElse(HeatmapDef())
      val start = gdef.startTime.toEpochMilli
      val end = gdef.endTime.toEpochMilli
      val xaxis = TimeAxis(Style.default, start, end, gdef.step)

      val (min, max) = bounds(start, end)
      val yaxis = LeftValueAxis(this, gdef.theme.axis, min, max)

      Some(Heatmap(settings, heatmapLines, xaxis, yaxis, gdef.height))
    } else {
      None
    }
  }
}
