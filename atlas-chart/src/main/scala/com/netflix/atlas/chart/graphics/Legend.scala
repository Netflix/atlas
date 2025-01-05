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

import java.awt.Font
import java.awt.Graphics2D
import com.netflix.atlas.chart.model.PlotDef

/**
  * Draws a legend for a given plot.
  *
  * @param styles
  *     Styles for elements on the legend.
  * @param plot
  *     Plot definition corresponding to the legend.
  * @param heatmap
  *     Heatmap entry to show on the legend. There is at most one heatmap for a given
  *     legend.
  * @param label
  *     Overall label to show for this legend.
  * @param showStats
  *     Whether or not to show basic line statistics for the legend entries.
  * @param maxEntries
  *     Maximum number of entries to show in the legend.
  */
case class Legend(
  styles: Styles,
  plot: PlotDef,
  heatmap: Option[Heatmap],
  label: Option[String],
  showStats: Boolean,
  maxEntries: Int
) extends Element
    with VariableHeight {

  private val numEntries = plot.legendData.size

  private val header = HorizontalPadding(5) :: label.toList.map { str =>
    val bold = ChartSettings.normalFont.deriveFont(Font.BOLD)
    val headerColor = plot.getAxisColor(styles.text.color)
    Text(str, font = bold, alignment = TextAlignment.LEFT, style = Style(headerColor))
  }

  private val heatmapEntry = heatmap.toList
    .flatMap { h =>
      List(HorizontalPadding(2), HeatmapLegendEntry(styles, plot, h, showStats))
    }

  private val entries = plot.legendData
    .take(maxEntries)
    .flatMap { data =>
      List(HorizontalPadding(2), LegendEntry(styles, plot, data, showStats))
    }

  private val footer =
    if (numEntries <= maxEntries) Nil
    else {
      val remaining = numEntries - maxEntries
      val txt =
        Text(s"... $remaining suppressed ...", alignment = TextAlignment.LEFT, style = styles.text)
      List(HorizontalPadding(2), txt)
    }

  private val block = Block(header ::: heatmapEntry ::: entries ::: footer)

  override def minHeight: Int = block.minHeight

  override def computeHeight(g: Graphics2D, width: Int): Int = block.computeHeight(g, width)

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    block.draw(g, x1, y1, x2, y2)
  }
}
