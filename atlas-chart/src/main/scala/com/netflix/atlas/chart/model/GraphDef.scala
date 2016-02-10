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
package com.netflix.atlas.chart.model

import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.graphics.Constants
import com.netflix.atlas.core.model.CollectorStats
import com.netflix.atlas.core.model.SummaryStats
import com.netflix.atlas.core.model.TimeSeries

/**
 * Definition of a time series graph.
 *
 * @param plots
 *     Plot definitions. Each plot has its own y-axis and set of lines.
 * @param startTime
 *     Start time (inclusive) for the first datapoint.
 * @param endTime
 *     End time (exclusive) for the last datapoint.
 * @param timezones
 *     Time zones to show as time axes on the chart. The first time zone in the list will be the
 *     primary used when dislaying time stamps or for formats that don't support multiple time
 *     zone rendering.
 * @param step
 *     Step size for each datapoint.
 * @param width
 *     Width in pixels for the chart area. This excludes axes and other padding. The final image
 *     size will get calculated using this width as a starting point.
 * @param height
 *     Height in pixels for the chart area. This excludes the title, time axis, legend, etc. The
 *     final image size will get calculated using this height as a starting point.
 * @param layout
 *     Layout mode to use for rendering the image. Default is CANVAS.
 * @param zoom
 *     Zoom factor to apply as a transform to the image.
 * @param title
 *     Title of the graph.
 * @param legendType
 *     How to show the legend when rendering the graph.
 * @param onlyGraph
 *     Show only the chart without other details like axes, legends, labels, etc.
 * @param numberFormat
 *     Pattern used for formatting the number values in text based outputs.
 * @param loadTime
 *     How long it took to load the data for the chart in milliseconds.
 * @param stats
 *     Stats on how much data was processed to render the chart.
 * @param warnings
 *     Warnings to display to the user.
 * @param source
 *     Used to provide metadata for how the graph definition was created. For example the uri
 *     input by the user.
 */
case class GraphDef(
    plots: List[PlotDef],
    startTime: Instant,
    endTime: Instant,
    timezones: List[ZoneId] = List(ZoneOffset.UTC),
    step: Long = 60000,
    width: Int = 400,
    height: Int = 200,
    layout: Layout = Layout.CANVAS,
    zoom: Double = 1.0,
    title: Option[String] = None,
    legendType: LegendType = LegendType.LABELS_WITH_STATS,
    onlyGraph: Boolean = false,
    numberFormat: String = "%f",
    loadTime: Long = -1,
    stats: CollectorStats = CollectorStats.unknown,
    warnings: List[String] = Nil,
    source: Option[String] = None) {

  /** Total number of lines for all plots. */
  val numLines = plots.foldLeft(0) { (acc, p) => acc + p.data.size }

  require(timezones.nonEmpty, "at least one timezone must be specified for the chart")

  /** Return the primary timezone to use for the graph. */
  def timezone: ZoneId = timezones.head

  /** Returns true if text should be shown. */
  def showText: Boolean = {
    width >= Constants.minWidthForText
  }

  /** Returns true if the legend should be shown. */
  def showLegend: Boolean = {
    !onlyGraph && legendTypeForLayout != LegendType.OFF && showText
  }

  /** Returns true if legend stats should be shown. */
  def showLegendStats: Boolean = {
    !onlyGraph && legendTypeForLayout == LegendType.LABELS_WITH_STATS
  }

  def legendTypeForLayout: LegendType = {
    if (layout.isFixedHeight) LegendType.OFF else legendType
  }

  /** Convert the defintion from a single axis to using one per line in the chart. */
  def axisPerLine: GraphDef = {
    require(plots.size == 1, "axisPerLine cannot be used with explicit multi axis")
    val plot = plots.head

    val size = plot.data.size
    if (size > GraphConstants.MaxYAxis) {
      val msg = s"Too many Y-axes, $size > ${GraphConstants.MaxYAxis}, axis per line disabled."
      copy(warnings = msg :: warnings)
    } else {
      val newPlots = plot.data.map { d => plot.copy(data = List(d), axisColor = None) }
      copy(plots = newPlots)
    }
  }

  /** Helper to map the plots for the graph. */
  def adjustPlots(f: PlotDef => PlotDef): GraphDef = {
    copy(plots = plots.map(f))
  }

  /** Helper to map the lines that are part of the graph. */
  def adjustLines(f: LineDef => LineDef): GraphDef = {
    val newPlots = plots.map { plot =>
      val newData = plot.data.map {
        case d: LineDef => f(d)
        case d          => d
      }
      plot.copy(data = newData)
    }
    copy(plots = newPlots)
  }

  /** Set the vision type for the graph to simulate types of color blindness. */
  def withVisionType(vt: VisionType): GraphDef = {
    val newPlots = plots.map { plot =>
      val newData = plot.data.map { d => d.withColor(vt.convert(d.color)) }
      plot.copy(data = newData, axisColor = plot.axisColor.map(vt.convert))
    }
    copy(plots = newPlots)
  }

  /** Return a new graph definition with the line stats filled in. */
  def computeStats: GraphDef = {
    val s = startTime.toEpochMilli
    val e = endTime.toEpochMilli
    adjustLines { line =>
      val stats = SummaryStats(line.data.data, s, e)
      line.copy(legendStats = stats)
    }
  }

  /** Return a new graph defintion with the lines bounded. */
  def bounded: GraphDef = {
    val s = startTime.toEpochMilli
    val e = endTime.toEpochMilli
    adjustLines { line =>
      val seq = line.data.data.bounded(s, e)
      line.copy(data = TimeSeries(line.data.tags, line.data.label, seq))
    }
  }
}
