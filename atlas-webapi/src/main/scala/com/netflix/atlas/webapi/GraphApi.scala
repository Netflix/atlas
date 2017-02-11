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
package com.netflix.atlas.webapi

import java.awt.Color
import java.time.Instant
import java.time.ZoneId

import akka.actor.ActorRefFactory
import akka.actor.Props
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.chart._
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model._
import com.netflix.atlas.core.model._
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Step
import com.netflix.atlas.core.util.Strings
import com.netflix.spectator.api.Spectator
import spray.http.HttpRequest
import spray.http.MediaType
import spray.http.Uri
import spray.routing.RequestContext


class GraphApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val registry = Spectator.globalRegistry()

  def routes: RequestContext => Unit = {
    path("api" / "v1" / "graph") {
      get { ctx =>
        try {
          val reqHandler = actorRefFactory.actorOf(Props(new GraphRequestActor(registry)))
          reqHandler.tell(GraphApi.toRequest(ctx.request), ctx.responder)
        } catch handleException(ctx)
      }
    }
  }
}

object GraphApi {

  private val interpreter = new Interpreter(ApiSettings.graphVocabulary.allWords)

  private val engines = ApiSettings.engines.map(e => e.name -> e).toMap

  private val contentTypes = engines.map { case (k, e) =>
    k -> MediaType.custom(e.contentType)
  }

  case class Request(
      query: String,
      start: Option[String],
      end: Option[String],
      timezones: List[String],
      step: Option[String],
      flags: ImageFlags,
      format: String,
      numberFormat: String,
      id: String,
      isBrowser: Boolean,
      isAllowedFromBrowser: Boolean,
      uri: String) {

    def shouldOutputImage: Boolean = (format == "png")

    val timezoneIds = {
      val zoneStrs = if (timezones.isEmpty) List(ApiSettings.timezone) else timezones
      zoneStrs.map { z => ZoneId.of(z) }
    }

    val tz: ZoneId = timezoneIds.head

    // Resolved start and end time
    val (resStart, resEnd) = timeRange(
      start.getOrElse(ApiSettings.startTime), end.getOrElse(ApiSettings.endTime), tz)

    val stepSize = {
      val datapointWidth = math.min(ApiSettings.maxDatapoints, flags.width)

      val stepDuration = step.map(Strings.parseDuration)
      val stepMillis = ApiSettings.stepSize
      val stepParam = stepDuration.fold(stepMillis)(s => Step.round(stepMillis, s.toMillis))
      Step.compute(stepParam, datapointWidth, resStart.toEpochMilli, resEnd.toEpochMilli)
    }

    // Final start and end time rounded to step boundaries
    val (fstart, fend) = roundToStep(resStart, resEnd)

    def startMillis: Long = fstart.toEpochMilli + stepSize
    def endMillis: Long = fend.toEpochMilli + stepSize

    private def timeRange(s: String, e: String, tz: ZoneId): (Instant, Instant) = {
      if (Strings.isRelativeDate(s, true) || s == "e") {
        require(!Strings.isRelativeDate(e, true), "start and end are both relative")
        val end = Strings.parseDate(e, tz)
        val start = Strings.parseDate(end, s, tz)
        start.toInstant -> end.toInstant
      } else {
        val start = Strings.parseDate(s, tz)
        val end = Strings.parseDate(start, e, tz)
        start.toInstant -> end.toInstant
      }
    }

    private def roundToStep(s: Instant, e: Instant): (Instant, Instant) = {
      val rs = roundToStep(s)
      val re = roundToStep(e)
      val adjustedStart = if (rs.equals(re)) rs.minusMillis(stepSize) else rs
      adjustedStart -> re
    }

    private def roundToStep(i: Instant): Instant = {
      Instant.ofEpochMilli(i.toEpochMilli / stepSize * stepSize)
    }

    def engine: GraphEngine = engines(format)

    def contentType: MediaType = contentTypes(format)

    val evalContext: EvalContext = {
      EvalContext(fstart.toEpochMilli, fend.toEpochMilli + stepSize, stepSize)
    }

    def exprs: List[StyleExpr] = {
      interpreter.execute(query).stack.reverse.flatMap {
        case ModelExtractors.PresentationType(s) => s.perOffset
      }
    }

    def toDbRequest: DataRequest = {
      val dataExprs = exprs.flatMap(_.expr.dataExprs)
      val deduped = dataExprs.toSet.toList
      DataRequest(evalContext, deduped)
    }

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
        numberFormat = numberFormat,
        plots = plots,
        source = if (ApiSettings.metadataEnabled) Some(uri) else None,
        warnings = warnings
      )

      gdef = gdef.withVisionType(flags.vision)
      if (flags.axisPerLine) gdef.axisPerLine else gdef
    }
  }

  case class Response(
      start: Long,
      step: Long,
      legend: List[String],
      metrics: List[Map[String, String]],
      values: Array[Array[Double]]) {

    private def toTimeSeries(label: String, pos: Int): TimeSeries = {
      val data = new Array[Double](values.length)
      var i = 0
      while (i < values.length) {
        data(i) = values(i)(pos)
        i += 1
      }
      TimeSeries(Map.empty, label, new ArrayTimeSeq(DsType.Gauge, start, step, data))
    }

    def toLineDefs: List[LineDef] = {
      legend.zipWithIndex.map { case (label, i) => LineDef(toTimeSeries(label, i)) }
    }

    def toGraphDef: GraphDef = {
      val plotDef = PlotDef(toLineDefs)

      GraphDef(
        startTime = Instant.ofEpochMilli(start),
        endTime = Instant.ofEpochMilli(start + step * values.length),
        step = step,
        plots = List(plotDef)
      )
    }
  }

  case class DataRequest(context: EvalContext, exprs: List[DataExpr])

  case class DataResponse(ts: Map[DataExpr, List[TimeSeries]])

  case class Axis(
      upper: Option[String] = None,
      lower: Option[String] = None,
      logarithmic: Boolean = false,
      stack: Boolean = false,
      ylabel: Option[String] = None,
      tickLabels: Option[String] = None,
      palette: Option[String] = None,
      sort: Option[String] = None,
      order: Option[String] = None)

  case class ImageFlags(
      title: Option[String],
      width: Int,
      height: Int,
      zoom: Double,
      axes: Map[Int, Axis],
      axisPerLine: Boolean,
      showLegend: Boolean,
      showLegendStats: Boolean,
      showOnlyGraph: Boolean,
      vision: VisionType,
      palette: String,
      layout: Layout)

  private def getAxisParam(params: Uri.Query, k: String, id: Int): Option[String] = {
    params.get(s"$k.$id").orElse(params.get(k))
  }

  private def newAxis(params: Uri.Query, id: Int): Axis = {
    Axis(
      upper = getAxisParam(params, "u", id),
      lower = getAxisParam(params, "l", id),
      logarithmic = getAxisParam(params, "o", id).contains("1"),
      stack = getAxisParam(params, "stack", id).contains("1"),
      ylabel = getAxisParam(params, "ylabel", id).filter(_ != ""),
      tickLabels = getAxisParam(params, "tick_labels", id),
      palette = params.get(s"palette.$id"),
      sort = getAxisParam(params, "sort", id),
      order = getAxisParam(params, "order", id))
  }

  def toRequest(req: HttpRequest): Request = {
    val params = req.uri.query
    val id = "default"

    import com.netflix.atlas.chart.GraphConstants._
    val axes = (0 to MaxYAxis).map(i => i -> newAxis(params, i)).toMap

    val vision = params.get("vision").map(v => VisionType.valueOf(v))

    val flags = ImageFlags(
      title = params.get("title").filter(_ != ""),
      width = params.get("w").fold(ApiSettings.width)(_.toInt),
      height = params.get("h").fold(ApiSettings.height)(_.toInt),
      zoom = params.get("zoom").fold(1.0)(_.toDouble),
      axes = axes,
      axisPerLine = params.get("axis_per_line").contains("1"),
      showLegend = !params.get("no_legend").contains("1"),
      showLegendStats = !params.get("no_legend_stats").contains("1"),
      showOnlyGraph = params.get("only_graph").contains("1"),
      vision = vision.getOrElse(VisionType.normal),
      palette = params.get("palette").getOrElse(ApiSettings.palette),
      layout = Layout.create(params.get("layout").getOrElse("canvas"))
    )

    val q = params.get("q")
    if (q.isEmpty) {
      throw new IllegalArgumentException("missing required parameter 'q'")
    }

    Request(
      query = q.get,
      start = params.get("s"),
      end = params.get("e"),
      timezones = params.getAll("tz").reverse,
      step = params.get("step"),
      flags = flags,
      format = params.get("format").getOrElse("png"),
      numberFormat = params.get("number_format").getOrElse("%f"),
      id = id,
      isBrowser = false,
      isAllowedFromBrowser = true,
      uri = req.uri.toString
    )
  }
}
