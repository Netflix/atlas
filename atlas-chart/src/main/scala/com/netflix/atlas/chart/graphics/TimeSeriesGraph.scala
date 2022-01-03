/*
 * Copyright 2014-2022 Netflix, Inc.
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
    val h =
      if (graphDef.height > max) max
      else {
        val min = GraphConstants.MinCanvasHeight
        if (graphDef.height < min) min else graphDef.height
      }
    if (graphDef.onlyGraph || graphDef.layout.isFixedHeight) h
    else {
      h + timeAxes.map(_.height).sum
    }
  }

  override def width: Int = {
    val max = GraphConstants.MaxWidth
    val w =
      if (graphDef.width > max) max
      else {
        val min = GraphConstants.MinCanvasWidth
        if (graphDef.width < min) min else graphDef.width
      }
    if (graphDef.onlyGraph || graphDef.layout.isFixedWidth) w
    else {
      val rightPadding = if (yaxes.tail.nonEmpty) 0 else TimeSeriesGraph.minRightSidePadding
      w + yaxes.map(_.width).sum + rightPadding
    }
  }

  val start: Long = graphDef.startTime.toEpochMilli
  val end: Long = graphDef.endTime.toEpochMilli

  val timeAxes: List[TimeAxis] = graphDef.timezones.zipWithIndex.map {
    case (tz, i) =>
      TimeAxis(
        Style(color = graphDef.theme.axis.line.color),
        start,
        end,
        graphDef.step,
        tz,
        if (i == 0) 40 else 0xff
      )
  }

  val timeAxis: TimeAxis = timeAxes.head

  val yaxes: List[ValueAxis] = graphDef.plots.zipWithIndex.map {
    case (plot, i) =>
      val bounds = plot.bounds(start, end)
      if (i == 0)
        LeftValueAxis(plot, graphDef.theme.axis, bounds._1, bounds._2)
      else
        RightValueAxis(plot, graphDef.theme.axis, bounds._1, bounds._2)
  }

  private def clip(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    g.setClip(x1, y1, x2 - x1, y2 - y1)
    g.setColor(graphDef.theme.canvas.background.color)
    g.fillRect(x1, y1, x2 - x1, y2 - y1)
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {

    val leftAxisW = yaxes.head.width
    val rightAxisW = yaxes.tail.foldLeft(0) { (acc, axis) =>
      acc + axis.width
    }
    val rightSideW = if (rightAxisW > 0) rightAxisW else TimeSeriesGraph.minRightSidePadding
    val axisW = leftAxisW + rightSideW
    val width = x2 - x1 - axisW

    val showAxes = !graphDef.onlyGraph && width >= GraphConstants.MinCanvasWidth
    val leftOffset = if (showAxes) leftAxisW else TimeSeriesGraph.minRightSidePadding
    val rightOffset = if (showAxes) rightSideW else TimeSeriesGraph.minRightSidePadding

    val timeAxisH = if (graphDef.onlyGraph) 10 else timeAxis.height
    val timeGrid = TimeGrid(timeAxis, graphDef.theme.majorGrid.line, graphDef.theme.minorGrid.line)

    val chartEnd = y2 - timeAxisH * timeAxes.size

    val prevClip = g.getClip
    clip(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd + 1)
    graphDef.plots.zip(yaxes).foreach {
      case (plot, axis) =>
        val offsets = TimeSeriesStack.Offsets(timeAxis)
        plot.lines.foreach { line =>
          val style = Style(color = line.color, stroke = new BasicStroke(line.lineWidth))
          val lineElement = line.lineStyle match {
            case LineStyle.LINE  => TimeSeriesLine(style, line.data.data, timeAxis, axis)
            case LineStyle.AREA  => TimeSeriesArea(style, line.data.data, timeAxis, axis)
            case LineStyle.VSPAN => TimeSeriesSpan(style, line.data.data, timeAxis)
            case LineStyle.STACK => TimeSeriesStack(style, line.data.data, timeAxis, axis, offsets)
          }

          lineElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
        }

        plot.horizontalSpans.foreach { hspan =>
          val style = Style(color = hspan.color)
          val spanElement = ValueSpan(style, hspan.v1, hspan.v2, axis)
          spanElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
        }

        plot.verticalSpans.foreach { vspan =>
          val style = Style(color = vspan.color)
          val spanElement = TimeSpan(style, vspan.t1.toEpochMilli, vspan.t2.toEpochMilli, timeAxis)
          spanElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
        }
    }
    g.setClip(prevClip)

    timeGrid.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)

    if (!graphDef.onlyGraph) {
      timeAxes.zipWithIndex.foreach {
        case (axis, i) =>
          val offset = chartEnd + 1 + timeAxisH * i
          axis.draw(g, x1 + leftOffset, offset, x2 - rightOffset, y2)
      }
    }

    val valueGrid =
      ValueGrid(yaxes.head, graphDef.theme.majorGrid.line, graphDef.theme.minorGrid.line)
    valueGrid.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
    if (showAxes) {
      yaxes.head.draw(g, x1, y1, x1 + leftAxisW - 1, chartEnd)
      yaxes.tail.zipWithIndex.foreach {
        case (axis, i) =>
          val offset = leftAxisW + width + leftAxisW * i
          axis.draw(g, x1 + offset, y1, x1 + offset + leftAxisW, chartEnd)
      }
    }
  }
}

object TimeSeriesGraph {

  /**
    * Allow at least 4 small characters on the right side to prevent the final tick mark label
    * from getting truncated.
    */
  private val minRightSidePadding = ChartSettings.smallFontDims.width * 4
}
