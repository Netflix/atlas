/*
 * Copyright 2014-2018 Netflix, Inc.
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
import java.awt.RenderingHints
import java.awt.image.BufferedImage
import java.awt.image.RenderedImage
import java.time.Duration
import java.time.ZonedDateTime

import com.netflix.atlas.chart.graphics.Constants
import com.netflix.atlas.chart.graphics.Element
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LegendType
import com.netflix.atlas.config.ConfigManager
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.core.util.UnitPrefix

class DefaultGraphEngine extends PngGraphEngine {

  private val renderingHints = {
    import scala.collection.JavaConversions._
    val config = ConfigManager.current.getConfig("atlas.chart.rendering-hints")
    config.entrySet.toList.map { entry =>
      val k = getField(entry.getKey).asInstanceOf[RenderingHints.Key]
      val v = getField(entry.getValue.unwrapped.asInstanceOf[String])
      k -> v
    }
  }

  private def getField(name: String): AnyRef = {
    classOf[RenderingHints].getField(name).get(null)
  }

  override def name: String = "png"

  override def createImage(gdef: GraphDef): RenderedImage = {
    import com.netflix.atlas.chart.graphics._

    val config = gdef.computeStats

    val notices = List.newBuilder[String]
    notices ++= config.warnings

    if (config.height > GraphConstants.MaxHeight) {
      notices += s"Restricted graph height to ${GraphConstants.MaxHeight}."
    }

    if (config.width > GraphConstants.MaxWidth) {
      notices += s"Restricted graph width to ${GraphConstants.MaxWidth}."
    }

    if (config.zoom > GraphConstants.MaxZoom) {
      notices += s"Restricted zoom to ${GraphConstants.MaxZoom}."
    }

    val aboveCanvas = List.newBuilder[Element]

    config.title.foreach { str =>
      if (config.showText)
        aboveCanvas += Text(str, font = Constants.largeFont).truncate(config.width)
    }
    aboveCanvas += HorizontalPadding(5)

    val hoffset = if (config.layout.isFixedHeight) height(aboveCanvas.result(), config.width) else 0
    val graph = TimeSeriesGraph(config.copy(height = config.height - hoffset))

    val belowCanvas = List.newBuilder[Element]
    if (config.showLegend) {
      val entriesPerPlot =
        if (config.numLines <= GraphConstants.MaxLinesInLegend) {
          GraphConstants.MaxLinesInLegend
        } else {
          GraphConstants.MaxLinesInLegend / config.plots.size
        }
      val showStats = config.showLegendStats && graph.width >= Constants.minWidthForStats
      belowCanvas += HorizontalPadding(5)
      if (config.plots.size > 1) {
        config.plots.zipWithIndex.foreach { case (plot, i) =>
          val label = plot.ylabel.map(s => s"Axis $i: $s").getOrElse(s"Axis $i")
          belowCanvas += Legend(plot, Some(label), showStats, entriesPerPlot)
        }
      } else {
        config.plots.foreach { plot =>
          belowCanvas += Legend(plot, None, showStats, entriesPerPlot)
        }
      }

      val start = config.startTime.toEpochMilli
      val end = config.endTime.toEpochMilli
      val frame = Strings.toString(Duration.between(config.startTime, config.endTime))
      val endTime = ZonedDateTime.ofInstant(config.endTime, config.timezone).toString
      val step = Strings.toString(Duration.ofMillis(config.step))
      val comment = "Frame: %s, End: %s, Step: %s".format(frame, endTime, step)
      belowCanvas += HorizontalPadding(15)
      belowCanvas += Text(comment, font = Constants.smallFont, alignment = TextAlignment.LEFT)

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
        belowCanvas += Text(stats, font = Constants.smallFont, alignment = TextAlignment.LEFT)
      } else if (config.loadTime > 0) {
        val stats = "Fetch: %sms".format(config.loadTime.toString)
        belowCanvas += Text(stats, font = Constants.smallFont, alignment = TextAlignment.LEFT)
      }
    }

    val noticeList = notices.result()
    if (noticeList.nonEmpty && config.showText) {
      val warnings = List.newBuilder[Element]
      warnings += Text("Warnings",
        font = Constants.normalFont.deriveFont(Font.BOLD),
        alignment = TextAlignment.LEFT)
      noticeList.foreach { notice =>
        warnings += HorizontalPadding(2)
        warnings += ListItem(Text(notice, alignment = TextAlignment.LEFT))
      }
      belowCanvas += HorizontalPadding(15)
      belowCanvas += Block(warnings.result(), background = Some(Color.ORANGE))
    }

    val bgColor =
      if (noticeList.nonEmpty && (!config.showText || config.layout.isFixedHeight))
        Color.ORANGE
      else
        Constants.backgroundColor

    val above = aboveCanvas.result()
    val below = belowCanvas.result()

    val parts = List.newBuilder[Element]
    parts ++= above
    parts += graph
    if (!config.layout.isFixedHeight)
      parts ++= below
    val elements = parts.result()

    val imgWidth = graph.width
    val imgHeight = height(elements, imgWidth)

    val zoom = if (config.zoom > GraphConstants.MaxZoom) GraphConstants.MaxZoom else config.zoom
    val zoomWidth = (imgWidth * zoom).toInt
    val zoomHeight = (imgHeight * zoom).toInt
    val image = new BufferedImage(zoomWidth, zoomHeight, BufferedImage.TYPE_INT_ARGB)
    val g = image.createGraphics()
    renderingHints.foreach(h => g.setRenderingHint(h._1, h._2))
    g.scale(zoom, zoom)
    g.setColor(bgColor)
    g.fillRect(0, 0, imgWidth, imgHeight)

    var y = 0
    elements.foreach { element =>
      val h = element.getHeight(Constants.refGraphics, imgWidth)
      element.draw(g, 0, y, imgWidth, y + h)
      y += h
    }

    image
  }

  private def height(elements: List[Element], w: Int): Int = {
    elements.foldLeft(0) { (acc, e) => acc + e.getHeight(Constants.refGraphics, w) }
  }
}

