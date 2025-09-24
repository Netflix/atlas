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
  * Draws a horizontal span from `v1` to `v2`.
  *
  * @param style
  *     Style to use for filling the span.
  * @param v1
  *     Starting value for the span.
  * @param v2
  *     Ending value for the span.
  * @param yaxis
  *     Axis used for creating the scale.
  */
case class ValueSpan(style: Style, v1: Double, v2: Double, yaxis: ValueAxis) extends Element {

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    val yscale = yaxis.scale(y1, y2)
    val p1 = yscale(v1)
    val p2 = yscale(v2)
    val py1 = math.min(p1, p2)
    val py2 = math.max(p1, p2)
    g.fillRect(x1, py1, x2 - x1, py2 - py1)
  }
}
