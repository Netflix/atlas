/*
 * Copyright 2014-2016 Netflix, Inc.
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

import java.awt.Font
import java.awt.Graphics2D

import com.netflix.atlas.chart.model.PlotDef

/**
 * Draws a legend for a given plot.
 *
 * @param plot
 *     Plot definition corresponding to the legend.
 * @param label
 *     Overall label to show for this legend.
 * @param showStats
 *     Whether or not to show basic line statistics for the legend entries.
 * @param maxEntries
 *     Maximum number of entries to show in the legend.
 */
case class Legend(
    plot: PlotDef,
    label: Option[String],
    showStats: Boolean,
    maxEntries: Int) extends Element with VariableHeight {

  private val header = HorizontalPadding(5) :: label.toList.map { str =>
    val bold = Constants.normalFont.deriveFont(Font.BOLD)
    val style = Style(color = plot.getAxisColor)
    Text(str, font = bold, alignment = TextAlignment.LEFT, style = style)
  }

  private val entries = plot.lines.take(maxEntries).flatMap { line =>
    List(HorizontalPadding(2), LegendEntry(plot, line, showStats))
  }

  private val footer = if (plot.lines.size <= maxEntries) Nil else {
    val remaining = plot.lines.size - maxEntries
    val txt = Text(s"... $remaining suppressed ...", alignment = TextAlignment.LEFT)
    List(HorizontalPadding(2), txt)
  }

  private val block = Block(header ::: entries ::: footer)

  override def minHeight: Int = block.minHeight

  override def computeHeight(g: Graphics2D, width: Int): Int = block.computeHeight(g, width)

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    block.draw(g, x1, y1, x2, y2)
  }
}
