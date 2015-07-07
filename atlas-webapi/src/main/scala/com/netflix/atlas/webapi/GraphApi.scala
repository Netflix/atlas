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
package com.netflix.atlas.webapi

import java.time.Instant
import java.time.ZoneId

import akka.actor.ActorRefFactory
import akka.actor.Props
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.chart._
import com.netflix.atlas.chart.model._
import com.netflix.atlas.core.model._
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Step
import com.netflix.atlas.core.util.Strings
import spray.http.HttpRequest
import spray.http.MediaType
import spray.http.Uri
import spray.routing.RequestContext


class GraphApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  def routes: RequestContext => Unit = {
    path("api" / "v1" / "graph") {
      get { ctx =>
        try {
          val reqHandler = actorRefFactory.actorOf(Props(new GraphRequestActor))
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
      timezone: Option[String],
      step: Option[String],
      flags: ImageFlags,
      format: String,
      numberFormat: String,
      id: String,
      isBrowser: Boolean,
      isAllowedFromBrowser: Boolean,
      includeMeta: Boolean) {

    def shouldOutputImage: Boolean = (format == "png")

    val tz: ZoneId = ZoneId.of(timezone.getOrElse(ApiSettings.timezone))

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

    def newGraphDef: GraphDef = {
      val legendType = (flags.showLegend, flags.showLegendStats) match {
        case (false, _) => LegendType.OFF
        case (_, false) => LegendType.LABELS_ONLY
        case (_, true)  => LegendType.LABELS_WITH_STATS
      }

      GraphDef(
        title = flags.title,
        timezone = tz,
        startTime = fstart.plusMillis(stepSize),
        endTime = fend.plusMillis(stepSize),
        step = stepSize,
        width = flags.width,
        height = flags.height,
        zoom = flags.zoom,
        legendType = legendType,
        onlyGraph = flags.showOnlyGraph,
        fontSize = flags.fontSize,
        numberFormat = numberFormat,
        plots = Nil
      )
    }

    def metadata: Map[String, String] = {
      val params = List.newBuilder[(String, String)]

      params ++= List(
        "tz"              -> tz.toString,
        "s"               -> fstart.toEpochMilli.toString,
        "start"           -> fstart.toString,
        "e"               -> fend.toEpochMilli.toString,
        "end"             -> fend.toString,
        "step"            -> stepSize.toString,
        "w"               -> flags.width.toString,
        "h"               -> flags.height.toString,
        "axis_per_line"   -> flags.axisPerLine.toString,
        "no_legend"       -> (!flags.showLegend).toString,
        "no_legend_stats" -> (!flags.showLegendStats).toString,
        "only_graph"      -> flags.showOnlyGraph.toString,
        "vision"          -> flags.vision.toString
      )

      flags.title.foreach { t => params += "title" -> t }

      params.result().toMap
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
      upper: Option[Double] = None,
      lower: Option[Double] = None,
      logarithmic: Boolean = false,
      stack: Boolean = false,
      ylabel: Option[String] = None)

  case class ImageFlags(
      title: Option[String],
      fontSize: Option[Int],
      width: Int,
      height: Int,
      zoom: Double,
      axes: Map[Int, Axis],
      axisPerLine: Boolean,
      showLegend: Boolean,
      showLegendStats: Boolean,
      showOnlyGraph: Boolean,
      vision: VisionType,
      palette: String)

  private def newAxis(params: Uri.Query, id: Int): Axis = {
    Axis(
      upper = params.get(s"u.$id").orElse(params.get("u")).map(_.toDouble),
      lower = params.get(s"l.$id").orElse(params.get("l")).map(_.toDouble),
      logarithmic = params.get(s"o.$id").orElse(params.get("o")).contains("1"),
      stack = params.get(s"stack.$id").orElse(params.get("stack")).contains("1"),
      ylabel = params.get(s"ylabel.$id").orElse(params.get("ylabel")).filter(_ != ""))
  }

  def toRequest(req: HttpRequest): Request = {
    val params = req.uri.query
    val id = "default"

    import com.netflix.atlas.chart.GraphConstants._
    val axes = (0 to MaxYAxis).map(i => i -> newAxis(params, i)).toMap

    val vision = params.get("vision").map(v => VisionType.valueOf(v))

    val flags = ImageFlags(
      title = params.get("title").filter(_ != ""),
      fontSize = params.get("font_size").map(_.toInt),
      width = params.get("w").fold(ApiSettings.width)(_.toInt),
      height = params.get("h").fold(ApiSettings.height)(_.toInt),
      zoom = params.get("zoom").fold(1.0)(_.toDouble),
      axes = axes,
      axisPerLine = params.get("axis_per_line").contains("1"),
      showLegend = !params.get("no_legend").contains("1"),
      showLegendStats = !params.get("no_legend_stats").contains("1"),
      showOnlyGraph = params.get("only_graph").contains("1"),
      vision = vision.getOrElse(VisionType.normal),
      palette = params.get("palette").getOrElse(ApiSettings.palette)
    )

    val q = params.get("q")
    if (!q.isDefined) {
      throw new IllegalArgumentException("missing required parameter 'q'")
    }

    Request(
      query = q.get,
      start = params.get("s"),
      end = params.get("e"),
      timezone = params.get("tz"),
      step = params.get("step"),
      flags = flags,
      format = params.get("format").getOrElse("png"),
      numberFormat = params.get("number_format").getOrElse("%f"),
      id = id,
      isBrowser = false,
      isAllowedFromBrowser = true,
      includeMeta = params.get("meta").contains("1"))
  }
}
