/*
 * Copyright 2015 Netflix, Inc.
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

import java.awt.BasicStroke
import java.awt.Color
import java.awt.Graphics2D

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineStyle

/**
 * Draws a time series graph.
 */
case class TimeSeriesGraph(graphDef: GraphDef) extends Element with FixedHeight with FixedWidth {
  override def height: Int = {
    val max = GraphConstants.MaxHeight
    val h = if (graphDef.height > max) max else graphDef.height
    h + timeAxis.height
  }

  override def width: Int = {
    val max = GraphConstants.MaxWidth
    val w = if (graphDef.width > max) max else graphDef.width
    val rightPadding = if (yaxes.tail.nonEmpty) 0 else TimeSeriesGraph.minRightSidePadding
    w + yaxes.map(_.width).sum + rightPadding
  }

  val start = graphDef.startTime.toEpochMilli
  val end = graphDef.endTime.toEpochMilli

  val timeAxis = TimeAxis(Style.default, start, end, graphDef.step, graphDef.timezone)

  val yaxes = graphDef.plots.zipWithIndex.map { case (plot, i) =>
    val scale = Scales.factory(plot.scale)
    val style = Style(color = plot.getAxisColor)
    val text = plot.ylabel.map { str => Text(str, style = style) }
    val bounds = plot.bounds(start, end)
    if (i == 0)
      LeftValueAxis(bounds._1, bounds._2, text, style = style, valueScale = scale)
    else
      RightValueAxis(bounds._1, bounds._2, text, style = style, valueScale = scale)
  }

  private def clip(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    g.setClip(x1, y1, x2 - x1, y2 - y1)
    g.setColor(Color.WHITE)
    g.fillRect(x1, y1, x2 - x1, y2 - y1)
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {

    val leftAxisW = yaxes.head.width
    val rightAxisW = yaxes.tail.foldLeft(0) { (acc, axis) => acc + axis.width }
    val rightSideW = if (rightAxisW > 0) rightAxisW else TimeSeriesGraph.minRightSidePadding
    val axisW = leftAxisW + rightSideW
    val width = x2 - x1 - axisW

    val timeAxisH = timeAxis.height
    val timeGrid = TimeGrid(timeAxis)

    val chartEnd = y2 - timeAxisH

    val prevClip = g.getClip
    clip(g, x1 + leftAxisW, y1, x2 - rightSideW, chartEnd + 1)
    graphDef.plots.zip(yaxes).foreach { case (plot, axis) =>

      val (regular, stacked) = plot.lines.partition(_.lineStyle != LineStyle.STACK)

      regular.foreach { line =>
        val style = Style(color = line.color, stroke = new BasicStroke(line.lineWidth))
        val lineElement = line.lineStyle match {
          case LineStyle.LINE  => TimeSeriesLine(style, line.data.data, timeAxis, axis)
          case LineStyle.AREA  => TimeSeriesArea(style, line.data.data, timeAxis, axis)
          case LineStyle.VSPAN => TimeSeriesSpan(style, line.data.data, timeAxis)
          case LineStyle.STACK => throw new IllegalStateException("invalid line style")
        }

        lineElement.draw(g, x1 + leftAxisW, y1, x2 - rightSideW, chartEnd)
      }

      if (stacked.nonEmpty) {
        val entries = stacked.map { line =>
          val style = Style(color = line.color, stroke = new BasicStroke(line.lineWidth))
          StackEntry(style, line.data.data)
        }
        val stackElement = TimeSeriesStack(entries, timeAxis, axis)
        stackElement.draw(g, x1 + leftAxisW, y1, x2 - rightSideW, chartEnd)
      }

      plot.horizontalSpans.foreach { hspan =>
        val style = Style(color = hspan.color)
        val spanElement = ValueSpan(style, hspan.v1, hspan.v2, axis)
        spanElement.draw(g, x1 + leftAxisW, y1, x2 - rightSideW, chartEnd)
      }

      plot.verticalSpans.foreach { vspan =>
        val style = Style(color = vspan.color)
        val spanElement = TimeSpan(style, vspan.t1.toEpochMilli, vspan.t2.toEpochMilli, timeAxis)
        spanElement.draw(g, x1 + leftAxisW, y1, x2 - rightSideW, chartEnd)
      }
    }
    g.setClip(prevClip)

    timeGrid.draw(g, x1 + leftAxisW, y1, x2 - rightSideW, chartEnd)
    timeAxis.draw(g, x1 + leftAxisW, chartEnd + 1, x2 - rightSideW, y2)

    val valueGrid = ValueGrid(yaxes.head)
    valueGrid.draw(g, x1 + leftAxisW, y1, x2 - rightSideW, chartEnd)
    yaxes.head.draw(g, x1, y1, x1 + leftAxisW - 1, chartEnd)
    yaxes.tail.zipWithIndex.foreach { case (axis, i) =>
      val offset = leftAxisW + width + leftAxisW * i
      axis.draw(g, x1 + offset, y1, x1 + offset + leftAxisW, chartEnd)
    }
  }
}

object TimeSeriesGraph {
  /**
   * Allow at least 4 small characters on the right side to prevent the final tick mark label
   * from getting truncated.
   */
  private val minRightSidePadding = Constants.smallFontDims.width * 4
}
