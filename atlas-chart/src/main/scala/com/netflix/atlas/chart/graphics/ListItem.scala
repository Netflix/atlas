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
  * Draws a list item, i.e., a bullet with wrapped text.
  */
case class ListItem(txt: Text) extends Element with VariableHeight {

  private val dims = ChartSettings.dimensions(txt.font)
  private val diameter = dims.width - 2

  override def minHeight: Int = txt.minHeight

  override def computeHeight(g: Graphics2D, width: Int): Int = {
    txt.computeHeight(g, width - dims.width)
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    txt.style.configure(g)
    val xoffset = (dims.width - diameter) / 2
    val yoffset = (dims.height - diameter) / 2
    g.fillOval(x1 + xoffset, y1 + yoffset, diameter, diameter)
    txt.draw(g, x1 + dims.width, y1, x2, y2)
  }
}
