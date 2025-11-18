/*
 * Copyright 2014-2025 Netflix, Inc.
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

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.chart.JsonCodec

import java.time.Instant
import java.time.temporal.ChronoUnit
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpEntity.ChunkStreamPart
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import org.apache.pekko.util.ByteString
import org.apache.pekko.util.Timeout
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.StatefulExpr
import com.netflix.atlas.core.model.TimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.graph.GraphConfig
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.webapi.GraphApi.DataRequest
import com.netflix.atlas.webapi.GraphApi.DataResponse

import java.io.StringWriter
import scala.util.Using

/**
  * Provides the SSE data stream payload for a fetch response. Fetch is an alternative
  * to the graph API that is meant for accessing the data rather than rendering as an
  * image. The response can be partitioned across both the set of expressions and across
  * time to allow more flexibility on the backend for tradeoffs between latency and
  * intermediate overhead. The graph API enforces strict limits on the sizes.
  */
object FetchRequestSource {

  private val chunkSize = ApiSettings.fetchChunkSize

  /**
    * Create an SSE source that can be used as the entity for the HttpResponse.
    */
  def apply(system: ActorRefFactory, graphCfg: GraphConfig): Source[ChunkStreamPart, NotUsed] = {
    import org.apache.pekko.pattern.*
    import scala.concurrent.duration.*

    val dbRef = system.actorSelection("/user/db")
    val chunks = {
      val step = graphCfg.roundedStepSize
      val (fstart, fend) = roundToStep(step, graphCfg.resStart, graphCfg.resEnd)
      EvalContext(fstart.toEpochMilli, fend.toEpochMilli, step)
        .partition(chunkSize * step, ChronoUnit.MILLIS)
    }

    val heartbeatSrc = Source
      .repeat(DiagnosticMessage.info("heartbeat"))
      .throttle(1, 10.seconds, 1, ThrottleMode.Shaping)

    val metadataSrc = createMetadataSource(graphCfg)

    val dataSrc = Source(chunks)
      .flatMapConcat { chunk =>
        val req = DataRequest(graphCfg).copy(context = chunk)
        val future = ask(dbRef, req)(Timeout(30.seconds))
        Source
          .future(future)
          .collect {
            case DataResponse(s, data) => DataChunk(chunk.increaseStep(s), data)
          }
      }
      .via(new EvalFlow(graphCfg))
      .flatMapConcat(ts => Source(ts))
      .recover {
        case t: Throwable => DiagnosticMessage.error(t)
      }
      .merge(heartbeatSrc, eagerComplete = true)
      .map(_.toJson)

    val closeSrc = Source.single(DiagnosticMessage.close).map(_.toJson)

    metadataSrc
      .concat(dataSrc.concat(closeSrc))
      .map { msg =>
        val bytes = ByteString(s"$prefix$msg$suffix")
        ChunkStreamPart(bytes)
      }
  }

  private def createMetadataSource(graphCfg: GraphConfig): Source[String, NotUsed] = {
    val usedAxes = graphCfg.exprs.map(_.axis.getOrElse(0)).toSet
    if (graphCfg.flags.presentationMetadataEnabled) {
      val graphDef = toJson(gen => JsonCodec.writeGraphDefMetadata(gen, graphCfg.newGraphDef(Nil)))
      val plots = graphCfg.flags.axes.filter(t => usedAxes.contains(t._1)).map {
        case (i, axis) => toJson(gen => JsonCodec.writePlotDefMetadata(gen, axis.newPlotDef(), i))
      }
      Source(graphDef :: plots.toList)
    } else {
      Source.empty
    }
  }

  private def toJson(encode: JsonGenerator => Unit): String = {
    Using.resource(new StringWriter()) { writer =>
      Using.resource(Json.newJsonGenerator(writer)) { gen =>
        encode(gen)
      }
      writer.toString
    }
  }

  /**
    * Returns an HttpResponse with an entity that is generated by the fetch source.
    */
  def createResponse(system: ActorRefFactory, graphCfg: GraphConfig): HttpResponse = {
    val source = apply(system, graphCfg)
    HttpResponse(
      status = StatusCodes.OK,
      entity = HttpEntity.Chunked(MediaTypes.`text/event-stream`, source)
    )
  }

  private def isAllNaN(seq: TimeSeq, s: Long, e: Long, step: Long): Boolean = {
    require(s <= e, "start must be <= end")
    val end = e / step * step
    var t = s / step * step
    while (t < end) {
      if (!seq(t).isNaN) return false
      t += step
    }
    true
  }

  // SSE message prefix and suffix. Fetch is simple and just emits data items with JSON.
  private val prefix = "data: "
  private val suffix = "\n\n"

  private def roundToStep(step: Long, s: Instant, e: Instant): (Instant, Instant) = {
    val rs = roundToStep(step, s)
    val re = roundToStep(step, e)
    val adjustedStart = if (rs.equals(re)) rs.minusMillis(step) else rs
    adjustedStart -> re
  }

  private def roundToStep(step: Long, i: Instant): Instant = {
    Instant.ofEpochMilli(i.toEpochMilli / step * step)
  }

  /**
    * Keeps track of state for stateful operators across the execution. Note, that the data
    * must be broken down and processed in the correct time order for the state processing to
    * be correct. This operator should get created a single time for a given execution so that
    * state is maintained overall.
    */
  private class EvalFlow(graphCfg: GraphConfig)
      extends GraphStage[FlowShape[DataChunk, List[TimeSeriesMessage]]] {

    private val in = Inlet[DataChunk]("EvalFlow.in")
    private val out = Outlet[List[TimeSeriesMessage]]("EvalFlow.out")

    override val shape: FlowShape[DataChunk, List[TimeSeriesMessage]] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with InHandler with OutHandler {

        private val metadata = graphCfg.flags.presentationMetadataEnabled

        private var state = Map.empty[StatefulExpr, Any]

        override def onPush(): Unit = {
          val chunk = grab(in)
          val cfg = graphCfg.withStep(chunk.context.step)
          val ts = cfg.exprs
            .flatMap { s =>
              val palette =
                if (metadata)
                  Some(cfg.flags.axisPalette(cfg.settings, s.axis.getOrElse(0)))
                else None
              val context = chunk.context.copy(state = state)
              val result = s.expr.eval(context, chunk.data)
              state = result.state
              result.data
                .filterNot(ts => isAllNaN(ts.data, context.start, context.end, context.step))
                .map(ts => TimeSeriesMessage(s, context, ts.withLabel(s.legend(ts)), palette))
            }
          push(out, ts)
        }

        override def onPull(): Unit = {
          pull(in)
        }

        setHandlers(in, out, this)
      }
    }
  }

  private case class DataChunk(context: EvalContext, data: DataMap)

  type DataMap = Map[DataExpr, List[TimeSeries]]
}
