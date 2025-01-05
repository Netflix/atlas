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

import java.awt.Color
import java.awt.Graphics2D

/**
  * Draws a set of elements vertically.
  *
  * @param elements
  *     Set of elements to draw within the block.
  * @param background
  *     Fill color to use for the background of the block. If not specified then the elements will
  *     be drawn directly over the existing content.
  */
case class Block(elements: List[Element], background: Option[Color] = None)
    extends Element
    with VariableHeight {

  override def minHeight: Int = 0

  override def computeHeight(g: Graphics2D, width: Int): Int = {
    elements.map(_.getHeight(g, width)).sum
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val width = x2 - x1
    val height = y2 - y1
    background.foreach { c =>
      g.setColor(c)
      g.fillRect(x1, y1, width, height)
    }

    var y = y1
    elements.foreach { element =>
      val h = element.getHeight(g, width)
      element.draw(g, x1, y, x1 + width, y + h)
      y += h
    }
  }
}
