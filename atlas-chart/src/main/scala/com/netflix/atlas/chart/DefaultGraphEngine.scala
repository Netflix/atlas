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
package com.netflix.atlas.chart

import java.awt.Color
import java.awt.Font
import java.awt.image.BufferedImage
import java.awt.image.RenderedImage
import java.time.Duration
import java.time.ZonedDateTime

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LegendType
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.core.util.UnitPrefix

class DefaultGraphEngine extends PngGraphEngine {

  override def name: String = "png"

  override def createImage(gdef: GraphDef): RenderedImage = {
    import com.netflix.atlas.chart.graphics._

    val config = gdef.computeStats

    val notices = List.newBuilder[String]
    notices ++= config.warnings

    val graphHeight = math.min(config.height, GraphConstants.MaxHeight)
    val graphWidth = math.min(config.width, GraphConstants.MaxWidth)
    if (config.height > GraphConstants.MaxHeight) {
      notices += s"Restricted graph height to ${GraphConstants.MaxHeight}."
    }

    if (config.width > GraphConstants.MaxWidth) {
      notices += s"Restricted graph width to ${GraphConstants.MaxWidth}."
    }

    val parts = List.newBuilder[Element]

    config.title.foreach { str =>
      parts += Text(str, font = Constants.largeFont)
    }
    parts += HorizontalPadding(5)

    val graph = TimeSeriesGraph(config)
    parts += graph

    if (config.legendType != LegendType.OFF) {
      if (config.numLines > GraphConstants.MaxLinesInLegend) {
        notices +=
          s"""
            |Too many lines, ${config.numLines} > ${GraphConstants.MaxLinesInLegend}, legend
            | was suppressed.
          """.stripMargin
      } else {
        parts += HorizontalPadding(5)
        if (config.plots.size > 1) {
          val bold = Constants.normalFont.deriveFont(Font.BOLD)
          config.plots.zipWithIndex.foreach { case (plot, i) =>
            parts += HorizontalPadding(5)
            val label = plot.ylabel.map(s => s"Axis $i: $s").getOrElse(s"Axis $i")
            parts += Text(label,
              font = bold,
              alignment = TextAlignment.LEFT,
              style = Style(color = plot.getAxisColor))
            plot.lines.foreach { line =>
              parts += HorizontalPadding(2)
              parts += LegendEntry(line, config.legendType == LegendType.LABELS_WITH_STATS)
            }
          }
        } else {
          config.plots.foreach { plot =>
            parts += HorizontalPadding(5)
            plot.lines.foreach { line =>
              parts += HorizontalPadding(2)
              parts += LegendEntry(line, config.legendType == LegendType.LABELS_WITH_STATS)
            }
          }
        }
      }
    }

    val start = config.startTime.toEpochMilli
    val end = config.endTime.toEpochMilli
    val frame = Strings.toString(Duration.between(config.startTime, config.endTime))
    val endTime = ZonedDateTime.ofInstant(config.endTime, config.timezone).toString
    val step = Strings.toString(Duration.ofMillis(config.step))
    val comment = "Frame: %s, End: %s, Step: %s".format(frame, endTime, step)
    parts += HorizontalPadding(15)
    parts += Text(comment, font = Constants.smallFont, alignment = TextAlignment.LEFT)

    if (config.loadTime > 0 && config.stats.inputLines > 0) {
      val graphLines = config.plots.map(_.data.size).sum
      val graphDatapoints = graphLines * ((end - start) / (config.step / 1000) + 1)
      val stats = "Fetch: %sms (L: %s, %s, %s; D: %s, %s, %s)".format(
        config.loadTime.toString,
        UnitPrefix.format(config.stats.inputLines),
        UnitPrefix.format(config.stats.outputLines),
        UnitPrefix.format(graphLines),
        UnitPrefix.format(config.stats.inputDatapoints),
        UnitPrefix.format(config.stats.outputDatapoints),
        UnitPrefix.format(graphDatapoints)
      )
      parts += Text(stats, font = Constants.smallFont, alignment = TextAlignment.LEFT)
    } else if (config.loadTime > 0) {
      val stats = "Fetch: %sms".format(config.loadTime.toString)
      parts += Text(stats, font = Constants.smallFont, alignment = TextAlignment.LEFT)
    }

    val noticeList = notices.result()
    if (noticeList.nonEmpty) {
      val warnings = List.newBuilder[Element]
      warnings += Text("Warnings", font = Constants.normalFont.deriveFont(Font.BOLD), alignment = TextAlignment.LEFT)
      noticeList.foreach { notice =>
        warnings += HorizontalPadding(2)
        warnings += ListItem(Text(notice, alignment = TextAlignment.LEFT))
      }
      parts += HorizontalPadding(15)
      parts += Block(warnings.result(), background = Some(Color.ORANGE))
    }

    val elements = parts.result()

    val imgWidth = graph.width
    val imgHeight = elements.foldLeft(0) { (acc, element) =>
      acc + element.getHeight(Constants.refGraphics, imgWidth)
    }

    val image = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB)
    val g = image.createGraphics()
    g.setColor(Constants.canvasBackgroundColor)
    g.fillRect(0, 0, imgWidth, imgHeight)

    var y = 0
    elements.foreach { element =>
      val h = element.getHeight(Constants.refGraphics, imgWidth)
      element.draw(g, 0, y, imgWidth, y + h)
      y += h
    }

    image
  }
}

