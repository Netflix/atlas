/*
 * Copyright 2014-2026 Netflix, Inc.
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

import java.awt.Color
import java.time.Instant
import java.time.ZoneId
import org.apache.pekko.http.scaladsl.model.ContentType
import com.netflix.atlas.chart.Colors
import com.netflix.atlas.chart.GraphEngine
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LegendType
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.util.Features
import com.netflix.atlas.core.util.Step
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.model.LineStyleMetadata
import com.netflix.atlas.pekko.CallerContext

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
  features: Features,
  isBrowser: Boolean,
  isAllowedFromBrowser: Boolean,
  uri: String,
  // Identity of the caller, populated from the request by the endpoint so downstream data access
  // can be attributed to a caller. Defaults to the anonymous caller when not established.
  caller: CallerContext = CallerContext.Anonymous
) {

  import GraphConfig.*

  def shouldOutputImage: Boolean = format == "png"

  val timezoneIds: List[ZoneId] = getTimeZoneIds(settings, timezones)

  val tz: ZoneId = timezoneIds.head

  // Resolved start and end time
  val (resStart, resEnd) =
    Strings.timeRange(start.getOrElse(settings.startTime), end.getOrElse(settings.endTime), tz)

  // Step util for block size
  private val stepUtil = Step.forBlockStep(settings.blockStep)

  /** Input step size rounded if necessary to a supported step. */
  val roundedStepSize: Long = {
    val stepDuration = step.map(Strings.parseDuration)
    val stepMillis = settings.stepSize
    stepDuration.fold(stepMillis)(s => stepUtil.round(stepMillis, s.toMillis))
  }

  /** Effective step size for the graph after adjusting based on the size and time window. */
  val stepSize: Long = {
    val datapointWidth = math.min(settings.maxDatapoints, flags.width)
    val stepParam = roundedStepSize
    stepUtil.compute(stepParam, datapointWidth, resStart.toEpochMilli, resEnd.toEpochMilli)
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
    Instant.ofEpochMilli(Math.floorDiv(i.toEpochMilli, stepSize) * stepSize)
  }

  def engine: GraphEngine = settings.engines(format)

  def contentType: ContentType = settings.contentTypes(format)

  val evalContext: EvalContext = {
    EvalContext(fstart.toEpochMilli, fend.toEpochMilli + stepSize, stepSize)
  }

  // The methods below determine the style for the lines of an expression. They are shared
  // between rendering graphs and the presentation metadata so that they will be consistent.

  /** Resolve a color setting, named colors such as `blue1` are mapped based on the theme. */
  def resolveColor(color: String): Color = settings.resolveColor(flags.theme, color)

  /** Palette for lines on an axis if the expression does not specify a palette or color. */
  def axisPalette(axis: Int): String = {
    flags.axes.get(axis).flatMap(_.palette).getOrElse(flags.palette)
  }

  /** Palette for lines of an expression that has an offset. */
  def offsetPalette: String = settings.offsetPalette(flags.theme)

  /**
    * Palette for the lines of an expression if it does not have an explicit color. The
    * value may have a `hash:` prefix to indicate that colors should be selected based on the
    * hash of the label rather than in order.
    */
  def linePalette(expr: StyleExpr): String = {
    expr.palette.getOrElse {
      if (expr.offset > 0L) offsetPalette else axisPalette(expr.axis.getOrElse(0))
    }
  }

  /** Line style for an expression, if not set it will be based on the axis settings. */
  def lineStyle(expr: StyleExpr): LineStyle = {
    expr.lineStyle.fold {
      val stack = flags.axes.get(expr.axis.getOrElse(0)).exists(_.stack)
      if (stack) LineStyle.STACK else LineStyle.LINE
    }(LineStyle.parse)
  }

  /** Apply the alpha setting for the expression, if any, to a color. */
  def withAlpha(expr: StyleExpr, color: Color): Color = {
    expr.alpha.fold(color)(a => Colors.withAlpha(color, a))
  }

  /** Explicit color for the lines of an expression, if any, with the alpha applied. */
  def explicitColor(expr: StyleExpr): Option[Color] = {
    expr.color.map(c => withAlpha(expr, resolveColor(c)))
  }

  /** Returns true if a heatmap palette is configured for the axis. */
  def hasHeatmapPalette(axis: Int): Boolean = {
    flags.axes.get(axis).exists(_.heatmapPalette.nonEmpty)
  }

  /**
    * Returns a function that maps the label for a line of the expression to the style for
    * the presentation metadata. Returns None if presentation metadata is not enabled for the
    * request. The settings are validated when creating the function, so it will fail if a
    * setting such as the palette is invalid.
    *
    * The style is consistent with the graph rendering, including the vision type, except that
    * palette colors are always selected based on the hash of the label. For a streaming
    * use-case the full set of lines is not known so they cannot be assigned in order.
    */
  def presentationStyle(expr: StyleExpr): Option[String => LineStyleMetadata] = {
    if (!flags.presentationMetadataEnabled) None
    else {
      val axis = expr.axis.getOrElse(0)
      val lineStyle = this.lineStyle(expr)
      val template = LineStyleMetadata(axis, Color.BLACK, lineStyle, expr.lineWidth)
      val style: String => LineStyleMetadata = explicitColor(expr) match {
        case Some(c) =>
          val metadata = template.copy(color = flags.vision.convert(c))
          _ => metadata
        case None =>
          // Alpha and vision type are applied to the palette up front so that the settings
          // are validated here and the colors are not recomputed for each line.
          val palette = {
            val p = Palette.create(linePalette(expr).stripPrefix("hash:"))
            expr.alpha.fold(p)(p.withAlpha).withVisionType(flags.vision)
          }
          if (lineStyle == LineStyle.HEATMAP) {
            // Heatmaps use a single color for the axis. If a heatmap palette is configured,
            // then the color is not used.
            val c =
              if (hasHeatmapPalette(axis)) withAlpha(expr, Color.BLACK)
              else palette.colors(s"heatmap$axis".hashCode)
            val metadata = template.copy(color = c)
            _ => metadata
          } else { label =>
            template.copy(color = palette.colors(label.hashCode))
          }
      }
      Some(style)
    }
  }

  def withStep(newStep: Long): GraphConfig = {
    if (newStep == stepSize)
      return this
    copy(
      step = Some(s"${newStep}ms"),
      start = Some(Strings.toEpochOffset(fstart)),
      end = Some(Strings.toEpochOffset(fend))
    )
  }

  def exprs: List[StyleExpr] = parsedQuery.get

  private def getGraphTags(plots: List[PlotDef]): Map[String, String] = {
    val lines = plots.flatMap(_.data).collect { case line: LineDef => line.data.tags }
    if (lines.isEmpty)
      Map.empty
    else
      lines.reduce { (a, b) =>
        a.toSet.intersect(b.toSet).toMap
      }
  }

  def newGraphDef(plots: List[PlotDef], warnings: List[String] = Nil): GraphDef = {
    val legendType = (flags.showLegend, flags.showLegendStats) match {
      case (false, _) => LegendType.OFF
      case (_, false) => LegendType.LABELS_ONLY
      case (_, true)  => LegendType.LABELS_WITH_STATS
    }

    val title = flags.title.map(s => Strings.substitute(s, getGraphTags(plots)))

    var gdef = GraphDef(
      title = title,
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
      themeName = flags.theme,
      plots = plots,
      source = if (settings.metadataEnabled) Some(uri) else None,
      warnings = warnings,
      renderingHints = flags.hints
    )

    gdef = gdef.withVisionType(flags.vision)
    if (flags.axisPerLine) useAxisPerLine(gdef) else gdef
  }

  private def useAxisPerLine(gdef: GraphDef): GraphDef = {
    val graphDef = gdef.axisPerLine
    val multiY = graphDef.useLineColorForMultiY
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
