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
  * Draws a time series as an area filled to zero. If values are positive the fill will be down to
  * zero. If values are negative the fill will be up to zero.
  *
  * @param style
  *     Style to use for drawing the area.
  * @param ts
  *     Data for the line.
  * @param xaxis
  *     Axis used to create the X scale.
  * @param yaxis
  *     Axis used to create the Y scale.
  */
case class TimeSeriesArea(style: Style, ts: TimeSeq, xaxis: TimeAxis, yaxis: ValueAxis)
    extends Element {

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    val step = ts.step
    val xscale = xaxis.scale(x1, x2)
    val yscale = yaxis.scale(y1, y2)
    val axisy = yscale(0.0)
    var t = xaxis.start
    while (t < xaxis.end) {
      val px1 = xscale(t - step)
      val px2 = xscale(t)
      val nv = ts(t)
      val ny = yscale(nv)
      val py1 = math.min(axisy, ny)
      val py2 = math.max(axisy, ny) + 1
      if (!nv.isNaN) g.fillRect(px1, py1, px2 - px1, py2 - py1)
      t += step
    }
  }
}
