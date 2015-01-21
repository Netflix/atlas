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

import java.io.ByteArrayOutputStream
import java.time.Duration

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.chart.AxisDef
import com.netflix.atlas.chart.LineStyle
import com.netflix.atlas.chart.PlotDef
import com.netflix.atlas.chart.SeriesDef
import com.netflix.atlas.core.model._
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Spectator
import spray.can.Http
import spray.http.MediaTypes._
import spray.http._


class GraphRequestActor extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi._

  private def queryInterpreter = new Interpreter(QueryVocabulary.words ::: StandardVocabulary.words)

  private val errorId = Spectator.registry().createId("atlas.graph.errorImages")

  private val dbRef = context.actorSelection("/user/db")

  private var request: Request = _
  private var responseRef: ActorRef = _

  def receive = {
    case v => try innerReceive(v) catch {
      case t: Exception if request != null && request.shouldOutputImage =>
        // When viewing a page in a browser an error response is not rendered. To make it more
        // clear to the user we return a 200 with the error information encoded into an image.
        sendErrorImage(t, request.flags.width, request.flags.height, sender())
      case t: Throwable =>
        DiagnosticMessage.handleException(sender())(t)
    }
  }

  def innerReceive: Receive = {
    case req: Request =>
      request = req
      responseRef = sender()
      dbRef.tell(req.toDbRequest, self)
    case DataResponse(data) => sendImage(data)
    case ev: Http.ConnectionClosed =>
      log.info("connection closed")
      context.stop(self)
  }

  private def sendErrorImage(t: Throwable, w: Int, h: Int, responder: ActorRef) {
    val simpleName = t.getClass.getSimpleName
    Spectator.registry().counter(errorId.withTag("error", simpleName)).increment()
    val msg = s"$simpleName: ${t.getMessage}"
    val image = HttpEntity(`image/png`, PngImage.error(msg, w, h).toByteArray)
    responder ! HttpResponse(status = StatusCodes.OK, entity = image)
  }

  private def sendImage(data: Map[DataExpr, List[TimeSeries]]): Unit = {
    val ts = data.values.flatten

    val seriesList = request.exprs.flatMap { s =>
      val ts = s.expr.eval(request.evalContext, data).data
      val exprStr = s.expr.toString
      val tmp = ts.map { t =>
        val offset = Strings.toString(Duration.ofMillis(s.offset))
        val newT = t.withTags(t.tags + (TagKey.offset -> offset))
        val series = new SeriesDef
        series.data = newT
        series.query = exprStr
        series.label = s.legend(newT)
        series.axis = s.axis
        series.color = s.color
        series.alpha = s.alpha
        series.style = s.lineStyle.fold(LineStyle.LINE)(s => LineStyle.valueOf(s.toUpperCase))
        series.lineWidth = s.lineWidth
        series.palette = if (s.offset > 0L) "bw" else request.flags.palette
        series
      }
      tmp.sortWith(_.label < _.label)
    }

    val graphDef = request.newGraphDef

    //default axis
    val axisDef = new AxisDef
    graphDef.axis = Map(0 -> axisDef)

    //add default axis definitions on the right side of the graph for any
    //series which specified axis but did not specify a corresponding axis definition
    seriesList.foreach { seriesDef =>
      val yaxis = seriesDef.axis.getOrElse(0)
      if (!graphDef.axis.contains(yaxis)) {
        val ax = new AxisDef
        ax.rightSide = true
        graphDef.axis += (yaxis -> ax)
      }
    }

    for ((yax, axdef) <- graphDef.axis) {
      val axis = request.flags.axes(yax)
      axdef.min = axis.lower
      axdef.max = axis.upper
      axdef.label = axis.ylabel
      axdef.logarithmic = axis.logarithmic
      axdef.stack = axis.stack
    }

    val plotDef = new PlotDef
    plotDef.series = seriesList
    graphDef.plots = List(plotDef)

    val baos = new ByteArrayOutputStream
    request.engine.write(graphDef, baos)

    val entity = HttpEntity(request.contentType, baos.toByteArray)
    responseRef ! HttpResponse(StatusCodes.OK, entity)
    context.stop(self)
  }

  private def sendJson(data: AnyRef): Unit = {
    val entity = HttpEntity(`application/json`, Json.encode(data))
    responseRef ! HttpResponse(StatusCodes.OK, entity)
    context.stop(self)
  }

  private def sendText(data: String): Unit = {
    val entity = HttpEntity(`text/plain`, data)
    responseRef ! HttpResponse(StatusCodes.OK, entity)
    context.stop(self)
  }
}

