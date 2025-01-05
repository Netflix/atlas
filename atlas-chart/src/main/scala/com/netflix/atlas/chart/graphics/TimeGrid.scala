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
  * Draws vertical grid lines based on a time axis.
  *
  * @param xaxis
  *     Axis to use for creating the scale and determining the the tick marks that correspond with
  *     the major grid lines.
  * @param major
  *     Style to use for drawing the major tick lines.
  * @param minor
  *     Style to use for drawing the minor tick lines.
  */
case class TimeGrid(xaxis: TimeAxis, major: Style, minor: Style) extends Element {

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val ticks = xaxis.ticks(x1, x2)
    val xscale = xaxis.scale(x1, x2)

    ticks.foreach { tick =>
      if (tick.major) major.configure(g) else minor.configure(g)
      val px = xscale(tick.timestamp)
      if (px > x1 && px < x2) {
        g.drawLine(px, y1, px, y2)
      }
    }
  }
}
