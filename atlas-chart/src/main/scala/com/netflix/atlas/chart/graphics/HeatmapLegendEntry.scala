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

import com.netflix.atlas.chart.model.PlotDef

import java.awt.Color
import java.awt.Graphics2D

/**
  * Draws a legend entry for a heatmap.
  *
  * @param styles
  *     Styles for elements on the legend entry.
  * @param plot
  *     Definition for the plot containing the line.
  * @param data
  *     Definition for the data element.
  * @param showStats
  *     If true then summary stats will be shown below the label for the line.
  */
case class HeatmapLegendEntry(styles: Styles, plot: PlotDef, data: Heatmap, showStats: Boolean)
    extends Element
    with FixedHeight {

  override def height: Int = {
    if (showStats)
      ChartSettings.normalFontDims.height * 3
    else
      ChartSettings.normalFontDims.height
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    // Label
    val label = data.settings.label.getOrElse("Heatmap")
    val txt = Text(label, alignment = TextAlignment.LEFT, style = styles.text)
    val truncated = txt.truncate(x2 - x1)
    truncated.draw(g, x1, y1, x2, y2)

    if (showStats) {
      // Color scale
      val colors = data.palette.colorArray
      val offset = y1 + ChartSettings.normalFontDims.height

      // Indent to line up with stats for a normal line
      val d = ChartSettings.normalFontDims.height - 4
      val indent = ChartSettings.smallFontDims.width * 4 + d + 4

      // Width of color scale should match the legend stats
      val scaleWidth = ChartSettings.smallFontDims.width * 38
      val w = scaleWidth / colors.length

      // Color boxes
      g.setColor(Color.WHITE)
      g.fillRect(x1 + indent, offset, w * colors.length, d)
      var i = 0
      while (i < colors.length) {
        g.setColor(colors(i))
        g.fillRect(x1 + indent + i * w, offset, w, d)
        i += 1
      }

      // Axis lines
      styles.line.configure(g)
      g.drawLine(x1 + indent, offset + d, x1 + indent + w * colors.length, offset + d)

      // Vertical ticks for color axis
      val txtW = Math.max(w, ChartSettings.smallFontDims.width * 7)
      val spaceForTicks = w >= txtW
      val colorTicks = data.colorTicks
      i = 0
      while (i < colorTicks.length) {
        val x = x1 + indent + w * i
        styles.line.configure(g)
        g.drawLine(x, offset + d, x, offset + d + 4)
        if (spaceForTicks || i == 0 || i == colorTicks.length - 1) {
          // If space is limited, only show lables for first and last tick
          drawLabel(g, x, offset + d + 4, txtW, colorTicks(i).label)
        }
        i += 1
      }
    }
  }

  private def drawLabel(g: Graphics2D, x: Int, y: Int, w: Int, label: String): Unit = {
    val txt = Text(
      label,
      ChartSettings.smallFont,
      TextAlignment.CENTER,
      styles.text
    )
    val txtH = txt.minHeight
    val xoffset = w / 2
    txt.draw(g, x - xoffset, y + 2, x + w - xoffset, y + 2 + txtH)
  }
}
