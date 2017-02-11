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
import java.io.ByteArrayOutputStream
import java.time.Duration

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.chart._
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model._
import com.netflix.atlas.core.model._
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Strings
import com.netflix.spectator.api.Registry
import spray.can.Http
import spray.http.MediaTypes._
import spray.http._


class GraphRequestActor(registry: Registry) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi._

  private val errorId = registry.createId("atlas.graph.errorImages")

  private val dbRef = context.actorSelection("/user/db")

  private var request: Request = _
  private var responseRef: ActorRef = _

  def receive = {
    case v => try innerReceive(v) catch {
      case t: Exception if request != null && request.shouldOutputImage =>
        // When viewing a page in a browser an error response is not rendered. To make it more
        // clear to the user we return a 200 with the error information encoded into an image.
        sendErrorImage(t, request.flags.width, request.flags.height)
      case t: Throwable =>
        DiagnosticMessage.handleException(responseRef)(t)
    }
  }

  def innerReceive: Receive = {
    case req: Request =>
      request = req
      responseRef = sender()
      dbRef.tell(req.toDbRequest, self)
    case DataResponse(data) =>
      sendImage(data)
    case ev: Http.ConnectionClosed =>
      log.info("connection closed")
      context.stop(self)
  }

  private def sendErrorImage(t: Throwable, w: Int, h: Int): Unit = {
    val simpleName = t.getClass.getSimpleName
    registry.counter(errorId.withTag("error", simpleName)).increment()
    val msg = s"$simpleName: ${t.getMessage}"
    val image = HttpEntity(`image/png`, PngImage.error(msg, w, h).toByteArray)
    responseRef ! HttpResponse(status = StatusCodes.OK, entity = image)
  }

  private def sendImage(data: Map[DataExpr, List[TimeSeries]]): Unit = {

    val warnings = List.newBuilder[String]

    val plotExprs = request.exprs.groupBy(_.axis.getOrElse(0))
    val multiY = plotExprs.size > 1

    val palette = Palette.create(request.flags.palette).iterator
    val shiftPalette = Palette.create("bw").iterator

    val start = request.startMillis
    val end = request.endMillis

    val plots = plotExprs.toList.sortWith(_._1 < _._1).map { case (yaxis, exprs) =>
      val axisCfg = request.flags.axes(yaxis)
      val dfltStyle = if (axisCfg.stack) LineStyle.STACK else LineStyle.LINE

      val axisPalette = axisCfg.palette.fold(palette) { v => Palette.create(v).iterator }

      var messages = List.empty[String]
      val lines = exprs.flatMap { s =>
        val result = s.expr.eval(request.evalContext, data)

        // Pick the last non empty message to appear. Right now they are only used
        // as a test for providing more information about the state of filtering. These
        // can quickly get complicated when used with other features. For example,
        // sorting can mix and match lines across multiple expressions. Also binary
        // math operations that combine the results of multiple filter expressions or
        // multi-level group by with filtered input. For now this is just an
        // experiment for the common simple case to see how it impacts usability
        // when dealing with filter expressions that remove some of the lines.
        if (result.messages.nonEmpty) messages = result.messages.take(1)

        val ts = result.data
        val labelledTS = ts.map { t =>
          val offset = Strings.toString(Duration.ofMillis(s.offset))
          val newT = t.withTags(t.tags + (TagKey.offset -> offset))
          newT.withLabel(s.legend(newT))
        }

        val lineDefs = labelledTS.sortWith(_.label < _.label).map { t =>
          val color = s.color.getOrElse {
            val c = if (s.offset > 0L) shiftPalette.next() else axisPalette.next()
            // Alpha setting if present will set the alpha value for the color automatically
            // assigned by the palette. If using an explicit color it will have no effect as the
            // alpha can be set directly using an ARGB hex format for the color.
            s.alpha.fold(c)(a => Colors.withAlpha(c, a))
          }

          LineDef(
            data = t,
            color = color,
            lineStyle = s.lineStyle.fold(dfltStyle)(s => LineStyle.valueOf(s.toUpperCase)),
            lineWidth = s.lineWidth,
            legendStats = SummaryStats(t.data, start, end))
        }

        // Lines must be sorted for presentation after the colors have been applied
        // using the palette. The colors selected should be stable regardless of the
        // sort order that is applied. Otherwise colors would change each time a user
        // changed the sort.
        sort(warnings, s.sortBy, s.useDescending, lineDefs)
      }

      // Apply sort based on URL parameters. This will take precedence over
      // local sort on an expression.
      val sortedLines = sort(warnings, axisCfg.sort, axisCfg.order.contains("desc"), lines)

      PlotDef(
        data = sortedLines ::: messages.map(s => MessageDef(s"... $s ...")),
        lower = axisCfg.lower.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
        upper = axisCfg.upper.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
        ylabel = axisCfg.ylabel,
        scale = if (axisCfg.logarithmic) Scale.LOGARITHMIC else Scale.LINEAR,
        axisColor = if (multiY) None else Some(Color.BLACK),
        tickLabelMode = axisCfg.tickLabels.fold(TickLabelMode.DECIMAL)(TickLabelMode.apply))
    }

    val graphDef = request.newGraphDef(plots, warnings.result())

    val baos = new ByteArrayOutputStream
    request.engine.write(graphDef, baos)

    val entity = HttpEntity(request.contentType, baos.toByteArray)
    responseRef ! HttpResponse(StatusCodes.OK, entity)
    context.stop(self)
  }

  private def sort(
      warnings: scala.collection.mutable.Builder[String, List[String]],
      sortBy: Option[String],
      useDescending: Boolean,
      lines: List[LineDef]): List[LineDef] = {

    import java.lang.{Double => JDouble}
    val sorted = sortBy.fold(lines) { mode =>
      val cmp: Function2[LineDef, LineDef, Boolean] = mode match {
        case "legend" => (a, b) => a.data.label < b.data.label
        case "min"    => (a, b) => JDouble.compare(a.legendStats.min, b.legendStats.min) < 0
        case "max"    => (a, b) => JDouble.compare(a.legendStats.max, b.legendStats.max) < 0
        case "avg"    => (a, b) => JDouble.compare(a.legendStats.avg, b.legendStats.avg) < 0
        case "count"  => (a, b) => a.legendStats.count < b.legendStats.count
        case "total"  => (a, b) => JDouble.compare(a.legendStats.total, b.legendStats.total) < 0
        case "last"   => (a, b) => JDouble.compare(a.legendStats.last, b.legendStats.last) < 0
        case order    =>
          warnings += s"Invalid sort mode '$order'. Using default of 'legend'."
          (a, b) => a.data.label < b.data.label
      }
      lines.sortWith(cmp)
    }
    if (useDescending) sorted.reverse else sorted
  }
}

