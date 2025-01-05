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

/**
  * Draws a vertical span from `t1` to `t2`.
  *
  * @param style
  *     Style to use for filling the span.
  * @param t1
  *     Start time in milliseconds since the epoch.
  * @param t2
  *     End time in milliseconds since the epoch.
  *     Step size in milliseconds.
  * @param xaxis
  *     Axis used for creating the scale.
  */
case class TimeSpan(style: Style, t1: Long, t2: Long, xaxis: TimeAxis) extends Element {

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    val xscale = xaxis.scale(x1, x2)
    val px1 = xscale(t1)
    val px2 = xscale(t2)
    g.fillRect(px1, y1, px2 - px1, y2 - y1)
  }
}
