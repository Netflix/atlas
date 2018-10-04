/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import akka.NotUsed
import javax.inject.Inject
import akka.actor.ActorRefFactory
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

class StreamApi @Inject()(
  sm: StreamSubscriptionManager,
  splitter: ExpressionSplitter,
  implicit val actorRefFactory: ActorRefFactory,
  registry: Registry
) extends WebApi
    with StrictLogging {

  import StreamApi._

  private implicit val ec = scala.concurrent.ExecutionContext.global
  private implicit val materializer = ActorMaterializer()

  private val reRegistrations = registry.counter("atlas.lwcapi.reRegistrations")

  def routes: Route = {
    endpointPath("lwc" / "api" / "v1" / "stream", Segment) { streamId =>
      get {
        complete(handleReq(streamId))
      }
    }
  }

  private def stepAlignedTime(step: Long): Long = {
    registry.clock().wallTime() / step * step
  }

  private def handleReq(streamId: String): HttpResponse = {

    // Drop any other connections that may already be using the same id
    sm.unregister(streamId).foreach { queue =>
      queue.offer(
        SSEShutdown(
          s"Dropped: another connection is using the same stream-id: $streamId",
          unsub = false
        )
      )
      queue.complete()
    }

    // Create queue to allow messages coming into /evaluate to be passed to this stream
    val (queue, pub) = StreamOps
      .queue[SSERenderable](registry, "StreamApi", 10000, OverflowStrategy.dropHead)
      .map(msg => ChunkStreamPart(ByteString(msg.toSSE) ++ suffix))
      .toMat(Sink.asPublisher[ChunkStreamPart](true))(Keep.both)
      .run()

    // Send initial setup messages
    val handler = new QueueHandler(streamId, queue)
    queue.offer(SSEHello(streamId, instanceId))
    sm.register(streamId, handler)

    // Heartbeat messages to ensure that the socket is never idle
    val heartbeatSrc = Source
      .repeat(NotUsed)
      .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
      .map { value =>
        // There is a race condition on reconnects where the new connection can come in
        // before the stream for the old connection is closed. The cleanup for the old
        // connection will then unregister the stream id. This is a short-term work around
        // to ensure running streams have a registered handler and track the number of
        // events that we see.
        if (sm.register(streamId, handler)) {
          logger.debug(s"re-registered handler for stream $streamId")
          reRegistrations.increment()
        }
        value
      }
      .flatMapConcat { _ =>
        val steps = sm
          .subscriptionsForStream(streamId)
          .map(_.metadata.frequency)
          .distinct
          .map { step =>
            val heartbeat = LwcHeartbeat(stepAlignedTime(step), step)
            ChunkStreamPart(SSEGenericJson("heartbeat", heartbeat).toSSE)
          }
        Source(steps)
      }

    val source = Source
      .fromPublisher(pub)
      .merge(heartbeatSrc)
      .via(StreamOps.monitorFlow(registry, "StreamApi"))
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) =>
            logger.debug(s"lost client for $streamId")
            sm.unregister(streamId)
          case Failure(t) =>
            logger.debug(s"lost client for $streamId", t)
            sm.unregister(streamId)
        }
      }
    val entity = HttpEntity.Chunked(MediaTypes.`text/event-stream`.toContentType, source)
    HttpResponse(StatusCodes.OK, entity = entity)
  }
}

trait SSERenderable {

  def toSSE: String

  def toJson: String
}

object StreamApi {

  private val instanceId = NetflixEnvironment.instanceId()

  private val suffix = ByteString("\r\n\r\n")

  case class ExpressionsRequest(expressions: List[ExpressionMetadata]) extends JsonSupport

  object ExpressionsRequest {

    def fromJson(json: String): ExpressionsRequest = {
      val decoded = Json.decode[ExpressionsRequest](json)
      if (decoded.expressions == null || decoded.expressions.isEmpty)
        throw new IllegalArgumentException("Missing or empty expressions array")
      decoded
    }
  }

  abstract class SSEMessage(msgType: String, what: String, content: JsonSupport)
      extends SSERenderable {

    def toSSE = s"$msgType: $what ${content.toJson}"

    def getWhat: String = what
  }

  // Hello message
  case class HelloContent(streamId: String, instanceId: String) extends JsonSupport

  case class SSEHello(streamId: String, instanceId: String)
      extends SSEMessage("info", "hello", HelloContent(streamId, instanceId)) {

    def toJson: String = {
      Json.encode(DiagnosticMessage.info(s"setup stream $streamId on $instanceId"))
    }
  }

  // Generic message string
  case class SSEGenericJson(what: String, msg: JsonSupport) extends SSEMessage("data", what, msg) {

    def toJson: String = msg.toJson
  }

  // Shutdown message
  case class ShutdownReason(reason: String) extends JsonSupport

  case class SSEShutdown(reason: String, private val unsub: Boolean = true)
      extends SSEMessage("info", "shutdown", ShutdownReason(reason)) {

    def toJson: String = {
      Json.encode(DiagnosticMessage.info(s"shutting down stream on $instanceId: $reason"))
    }

    def shouldUnregister: Boolean = unsub
  }

  // Subscribe message
  case class SubscribeContent(expression: String, metrics: List[ExpressionMetadata])
      extends JsonSupport

  case class SSESubscribe(expr: String, metrics: List[ExpressionMetadata])
      extends SSEMessage("info", "subscribe", SubscribeContent(expr, metrics)) {

    def toJson: String = {
      Json.encode(
        LwcSubscription(expr, metrics.map(m => LwcDataExpr(m.id, m.expression, m.frequency)))
      )
    }
  }

  case class SSEMetricContent(timestamp: Long, id: String, tags: EvaluateApi.TagMap, value: Double)
      extends JsonSupport

  // Evaluate message
  case class SSEMetric(timestamp: Long, data: EvaluateApi.Item)
      extends SSEMessage(
        "data",
        "metric",
        SSEMetricContent(timestamp, data.id, data.tags, data.value)
      ) {

    def toJson: String = {
      Json.encode(LwcDatapoint(timestamp, data.id, data.tags, data.value))
    }
  }
}
