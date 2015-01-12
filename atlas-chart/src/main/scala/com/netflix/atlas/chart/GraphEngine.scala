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
import java.io.OutputStream
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

import com.fasterxml.jackson.core.JsonFactory
import com.netflix.atlas.core.model.CollectorStats
import com.netflix.atlas.core.model.SummaryStats
import com.netflix.atlas.core.model.TimeSeries


case class HSpan(axis: Int, v1: Double, v2: Double, c: Color, label: Option[String]) {
  def color = new Color(c.getRed, c.getGreen, c.getBlue, 50)
}
case class VSpan(t1: Instant, t2: Instant, c: Color, label: Option[String]) {
  def color = new Color(c.getRed, c.getGreen, c.getBlue, 50)
}

/**
 * Settings for a time-series graph.
 */
class GraphDef {
  var title: Option[String] = None

  var timezone: ZoneId = ZoneOffset.UTC
  var startTime: Instant = Instant.now().minus(3, ChronoUnit.HOURS)
  var endTime: Instant = Instant.now()

  var step: Long = 60000

  /**
   * Overrides other show parameters to display only the graph with no other visual clutter.
   * typically used for generating thumbnails or other small versions of the graphs that will
   * be scanned with many on a page.
   */
  var onlyGraph: Boolean = false

  var axisPerLine: Boolean = false

  var showBorder: Boolean = true

  var showLegend: Boolean = true
  var showLegendStats: Boolean = true

  var width: Int = 400
  var height: Int = 200

  var axis: Map[Int, AxisDef] = Map(0 -> new AxisDef)

  var plots: List[PlotDef] = Nil

  var verticalSpans: List[VSpan] = Nil

  var fontSize: Option[Int] = None

  /** Number format for textual representations of raw values. */
  var numberFormat: String = "%f"

  var loadTime: Long = -1L
  var stats: CollectorStats = CollectorStats.unknown

  var notices: List[Notice] = Nil

  var visionType: VisionType = VisionType.normal
}

/**
 * Settings for a particular y-axis.
 */
class AxisDef(var rightSide: Boolean) {
  def this() = this(false)
  var logarithmic: Boolean = false
  var stack: Boolean = false
  var label: Option[String] = None
  var min: Option[Double] = None
  var max: Option[Double] = None
}

/**
 * Settings for the data plotted on a particular y-axis.
 */
class PlotDef {

  var series: List[SeriesDef] = Nil

  var horizontalSpans: List[HSpan] = Nil
}

/**
 * Settings for a individual line.
 */
class SeriesDef {
  var tags: Map[String, String] = Map.empty
  var axis: Option[Int]  = None
  var label: String = "-"
  var color: Option[Color] = None
  var alpha: Option[Int] = None
  var palette: String = "default"
  var style: LineStyle = LineStyle.LINE
  var lineWidth: Float = 1.0f
  var data: TimeSeries = null

  var legendStats: SummaryStats = SummaryStats.empty
  var query: String = null
}

object GraphEngine {
  val jsonFactory = new JsonFactory
}

trait GraphEngine {
  def name: String
  def contentType: String
  def write(config: GraphDef, output: OutputStream)
}
