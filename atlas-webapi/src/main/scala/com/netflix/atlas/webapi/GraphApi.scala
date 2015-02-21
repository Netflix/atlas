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
import com.netflix.atlas.chart.GraphDef
import com.netflix.atlas.chart.GraphEngine
import com.netflix.atlas.chart.VisionType
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.util.Step
import com.netflix.atlas.core.util.Strings
import spray.http.HttpRequest
import spray.http.MediaType
import spray.http.Uri
import spray.routing.RequestContext


class GraphApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  import com.netflix.atlas.webapi.GraphApi._

  def routes: RequestContext => Unit = {
    path("api" / "v1" / "graph") {
      get { ctx =>
        try {
          val reqHandler = actorRefFactory.actorOf(Props(new GraphRequestActor))
          reqHandler.tell(toRequest(ctx.request), ctx.responder)
        } catch handleException(ctx)
      }
    }
  }

  private def toRequest(req: HttpRequest): Request = {
    val params = req.uri.query
    val id = "default"

    import com.netflix.atlas.chart.GraphConstants._
    val axes = (0 to MaxYAxis).map(i => i -> newAxis(params, i)).toMap

    val vision = params.get("vision").map(v => VisionType.valueOf(v))

    val flags = ImageFlags(
      title = params.get("title"),
      fontSize = params.get("font_size").map(_.toInt),
      width = params.get("w").fold(ApiSettings.width)(_.toInt),
      height = params.get("h").fold(ApiSettings.height)(_.toInt),
      axes = axes,
      axisPerLine = params.get("axis_per_line").contains("1"),
      showLegend = !params.get("no_legend").contains("1"),
      showLegendStats = !params.get("no_legend_stats").contains("1"),
      showBorder = !params.get("no_border").contains("1"),
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
      isAllowedFromBrowser = true)
  }

  private def newAxis(params: Uri.Query, id: Int): Axis = {
    Axis(
      upper = params.get(s"u.$id").orElse(params.get("u")).map(_.toDouble),
      lower = params.get(s"l.$id").orElse(params.get("l")).map(_.toDouble),
      logarithmic = params.get(s"o.$id").orElse(params.get("o")) == Some("1"),
      stack = params.get(s"stack.$id").orElse(params.get("stack")) == Some("1"),
      ylabel = params.get(s"ylabel.$id").orElse(params.get("ylabel")))
  }

}

object GraphApi {

  private val interpreter = new Interpreter(StyleVocabulary.allWords ::: StandardVocabulary.allWords)

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
      isAllowedFromBrowser: Boolean) {

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
    val fstart = roundToStep(resStart)
    val fend = roundToStep(resEnd)

    private def timeRange(s: String, e: String, tz: ZoneId): (Instant, Instant) = {
      if (Strings.isRelativeDate(s, true)) {
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

    private def roundToStep(t: (Instant, Instant)): (Instant, Instant) = {
      roundToStep(t._1) -> roundToStep(t._2)
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
      val graphDef = new GraphDef
      graphDef.title = flags.title
      graphDef.timezone = tz
      graphDef.startTime = fstart
      graphDef.endTime = fend
      graphDef.step = stepSize
      graphDef.width = flags.width
      graphDef.height = flags.height
      graphDef.axisPerLine = flags.axisPerLine
      graphDef.showLegend = flags.showLegend
      graphDef.showLegendStats = flags.showLegendStats
      graphDef.showBorder = flags.showBorder
      graphDef.onlyGraph = flags.showOnlyGraph
      graphDef.fontSize  = flags.fontSize
      graphDef.numberFormat = numberFormat
      graphDef.visionType = flags.vision
      graphDef
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
      axes: Map[Int, Axis],
      axisPerLine: Boolean,
      showLegend: Boolean,
      showLegendStats: Boolean,
      showBorder: Boolean,
      showOnlyGraph: Boolean,
      vision: VisionType,
      palette: String)
}
