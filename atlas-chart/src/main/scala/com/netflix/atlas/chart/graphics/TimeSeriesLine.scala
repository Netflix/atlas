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
  * Draws a time series as a stepped line.
  *
  * @param style
  *     Style to use for drawing the line.
  * @param ts
  *     Data for the line.
  * @param xaxis
  *     Axis used to create the X scale.
  * @param yaxis
  *     Axis used to create the Y scale.
  */
case class TimeSeriesLine(style: Style, ts: TimeSeq, xaxis: TimeAxis, yaxis: ValueAxis)
    extends Element {

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    val step = ts.step
    val xscale = xaxis.scale(x1, x2)
    val yscale = yaxis.scale(y1, y2)
    var t = xaxis.start
    var pv = ts(t)
    while (t < xaxis.end) {
      val px1 = xscale(t - step)
      val px2 = xscale(t)
      val nv = ts(t)
      val py = yscale(pv)
      val ny = yscale(nv)
      if (!pv.isNaN && !nv.isNaN) g.drawLine(px1, py, px1, ny)
      if (!nv.isNaN) g.drawLine(px1, ny, px2, ny)
      t += step
      pv = nv
    }
  }
}
