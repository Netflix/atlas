/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.eval.graph

import java.time.Instant
import java.time.ZoneId

import akka.http.scaladsl.model.ContentType
import com.netflix.atlas.chart.GraphEngine
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LegendType
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.util.Step
import com.netflix.atlas.core.util.Strings

import scala.util.Try

case class GraphConfig(
  settings: DefaultSettings,
  query: String,
  parsedQuery: Try[List[StyleExpr]],
  start: Option[String],
  end: Option[String],
  timezones: List[String],
  step: Option[String],
  flags: ImageFlags,
  format: String,
  id: String,
  isBrowser: Boolean,
  isAllowedFromBrowser: Boolean,
  uri: String
) {

  import GraphConfig._

  def shouldOutputImage: Boolean = (format == "png")

  val timezoneIds: List[ZoneId] = getTimeZoneIds(settings, timezones)

  val tz: ZoneId = timezoneIds.head

  // Resolved start and end time
  val (resStart, resEnd) =
    Strings.timeRange(start.getOrElse(settings.startTime), end.getOrElse(settings.endTime), tz)

  /** Input step size rounded if necessary to a supported step. */
  val roundedStepSize: Long = {
    val stepDuration = step.map(Strings.parseDuration)
    val stepMillis = settings.stepSize
    stepDuration.fold(stepMillis)(s => Step.round(stepMillis, s.toMillis))
  }

  /** Effective step size for the graph after adjusting based on the size and time window. */
  val stepSize: Long = {
    val datapointWidth = math.min(settings.maxDatapoints, flags.width)
    val stepParam = roundedStepSize
    Step.compute(stepParam, datapointWidth, resStart.toEpochMilli, resEnd.toEpochMilli)
  }

  // Final start and end time rounded to step boundaries
  val (fstart, fend) = roundToStep(resStart, resEnd)

  def startMillis: Long = fstart.toEpochMilli + stepSize

  def endMillis: Long = fend.toEpochMilli + stepSize

  private def roundToStep(s: Instant, e: Instant): (Instant, Instant) = {
    val rs = roundToStep(s)
    val re = roundToStep(e)
    val adjustedStart = if (rs.equals(re)) rs.minusMillis(stepSize) else rs
    adjustedStart -> re
  }

  private def roundToStep(i: Instant): Instant = {
    Instant.ofEpochMilli(i.toEpochMilli / stepSize * stepSize)
  }

  def engine: GraphEngine = settings.engines(format)

  def contentType: ContentType = settings.contentTypes(format)

  val evalContext: EvalContext = {
    EvalContext(fstart.toEpochMilli, fend.toEpochMilli + stepSize, stepSize)
  }

  def exprs: List[StyleExpr] = parsedQuery.get

  def newGraphDef(plots: List[PlotDef], warnings: List[String] = Nil): GraphDef = {
    val legendType = (flags.showLegend, flags.showLegendStats) match {
      case (false, _) => LegendType.OFF
      case (_, false) => LegendType.LABELS_ONLY
      case (_, true)  => LegendType.LABELS_WITH_STATS
    }

    var gdef = GraphDef(
      title = flags.title,
      timezones = timezoneIds,
      startTime = fstart.plusMillis(stepSize),
      endTime = fend.plusMillis(stepSize),
      step = stepSize,
      width = flags.width,
      height = flags.height,
      layout = flags.layout,
      zoom = flags.zoom,
      legendType = legendType,
      onlyGraph = flags.showOnlyGraph,
      plots = plots,
      source = if (settings.metadataEnabled) Some(uri) else None,
      warnings = warnings
    )

    gdef = gdef.withVisionType(flags.vision)
    if (flags.axisPerLine) useAxisPerLine(gdef) else gdef
  }

  private def useAxisPerLine(gdef: GraphDef): GraphDef = {
    val graphDef = gdef.axisPerLine
    val multiY = graphDef.plots.size > 1
    val plots = graphDef.plots.zipWithIndex.map {
      case (p, i) =>
        flags.axes(i).newPlotDef(p.data, multiY)
    }
    graphDef.copy(plots = plots)
  }
}

object GraphConfig {

  private[graph] def getTimeZoneIds(
    settings: DefaultSettings,
    timezones: List[String]
  ): List[ZoneId] = {
    val zoneStrs = if (timezones.isEmpty) List(settings.timezone) else timezones
    zoneStrs.map(ZoneId.of)
  }
}
