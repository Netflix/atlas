/*
 * Copyright 2014-2017 Netflix, Inc.
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

import com.netflix.atlas.chart.model.DataDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.MessageDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.TickLabelMode
import com.netflix.atlas.core.util.UnitPrefix

/**
  * Draws a legend entry for a line.
  *
  * @param plot
  *     Definition for the plot containing the line.
  * @param data
  *     Definition for the data element.
  * @param showStats
  *     If true then summary stats will be shown below the label for the line.
  */
case class LegendEntry(plot: PlotDef, data: DataDef, showStats: Boolean)
    extends Element with FixedHeight {

  private def shouldShowStats: Boolean = showStats && data.isInstanceOf[LineDef]

  override def height: Int = {
    if (!shouldShowStats) Constants.normalFontDims.height else {
      Constants.normalFontDims.height + Constants.smallFontDims.height * 3
    }
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    if (data.isInstanceOf[MessageDef]) {
      // Draw the label
      val txt = Text(data.label, style = Style(data.color), alignment = TextAlignment.LEFT)
      val truncated = txt.truncate(x2 - x1)
      truncated.draw(g, x1, y1, x2, y2)
    } else {
      val d = Constants.normalFontDims.height - 4

      // Draw the color box for the legend entry. If the color has an alpha setting, then the
      // background can impact the color so we first fill with white to make it the same as the
      // background color of the chart.
      g.setColor(Color.WHITE)
      g.fillRect(x1 + 2, y1 + 2, d, d)
      g.setColor(data.color)
      g.fillRect(x1 + 2, y1 + 2, d, d)

      // Border for the color box
      g.setColor(Color.BLACK)
      g.drawRect(x1 + 2, y1 + 2, d, d)

      // Draw the label
      val txt = Text(data.label, alignment = TextAlignment.LEFT)
      val truncated = txt.truncate(x2 - x1 - d - 4)
      truncated.draw(g, x1 + d + 4, y1, x2, y2)

      data match {
        case line: LineDef if showStats =>
          val stats = line.legendStats
          val max = format(stats.max)
          val min = format(stats.min)
          val avg = format(stats.avg)
          val last = format(stats.last)
          val total = format(stats.total)
          val count = format(stats.count)
          val rows = List(
            "    Max : %s    Min  : %s".format(max, min),
            "    Avg : %s    Last : %s".format(avg, last),
            "    Tot : %s    Cnt  : %s".format(total, count)
          )
          val offset = y1 + Constants.normalFontDims.height
          val rowHeight = Constants.smallFontDims.height
          rows.zipWithIndex.foreach { case (row, i) =>
            val txt = Text(row, font = Constants.smallFont, alignment = TextAlignment.LEFT)
            txt.draw(g, x1 + d + 4, offset + i * rowHeight, x2, y2)
          }
        case _ =>
      }
    }
  }

  private def format(v: Double): String = {
    plot.tickLabelMode match {
      case TickLabelMode.BINARY => UnitPrefix.binary(v).format(v, "%9.2f%1s", "%8.1e ")
      case _                    => UnitPrefix.decimal(v).format(v, "%9.3f%1s", "%8.1e ")
    }
  }
}

