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
package com.netflix.atlas.chart.graphics

import java.awt.Graphics2D

import com.netflix.atlas.core.model.TimeSeq

/**
  * Draws a set of time series stacked on top of each other. The style will be the same as area
  * only it will fill to the previous line rather than zero.
  *
  * @param style
  *     Style to use for drawing the line.
  * @param ts
  *     Data for the line.
  * @param xaxis
  *     Axis used to create the X scale.
  * @param yaxis
  *     Axis used to create the Y scale.
  * @param offsets
  *     Y-offsets to use and update when stacking the line.
  */
case class TimeSeriesStack(
  style: Style,
  ts: TimeSeq,
  xaxis: TimeAxis,
  yaxis: ValueAxis,
  offsets: TimeSeriesStack.Offsets
) extends Element {

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val step = xaxis.step
    val xscale = xaxis.scale(x1, x2)
    val yscale = yaxis.scale(y1, y2)
    var t = xaxis.start
    while (t < xaxis.end) {
      val px1 = xscale(t - step)
      val px2 = xscale(t)
      val ox = ((t - xaxis.start) / step).toInt
      val posY = offsets.posY(ox)
      val negY = offsets.negY(ox)
      style.configure(g)
      ts(t) match {
        case v if v == 0.0 && posY == 0.0 && negY == 0.0 =>
          // Provides a line along the xaxis to avoid confusion between 0 and NaN (no data)
          val py1 = yscale(posY)
          g.fillRect(px1, py1, px2 - px1, 1)
        case v if v > 0.0 =>
          val axisy = yscale(posY)
          val py = yscale(v + posY)
          val py1 = math.min(axisy, py)
          val py2 = math.max(axisy, py) + 1
          g.fillRect(px1, py1, px2 - px1, py2 - py1)
          offsets.posY(ox) = v + posY
        case v if v < 0.0 =>
          val axisy = yscale(negY)
          val py = yscale(v + negY)
          val py1 = math.min(axisy, py)
          val py2 = math.max(axisy, py) + 1
          g.fillRect(px1, py1, px2 - px1, py2 - py1)
          offsets.negY(ox) = v + negY
        case _ =>
      }
      t += step
    }
  }
}

object TimeSeriesStack {

  /** Stacked offsets for each time interval in the chart. */
  case class Offsets(posY: Array[Double], negY: Array[Double])

  object Offsets {

    def apply(axis: TimeAxis): Offsets = {
      val size = ((axis.end - axis.start) / axis.step).toInt
      val posY = new Array[Double](size)
      val negY = new Array[Double](size)
      Offsets(posY, negY)
    }
  }
}
