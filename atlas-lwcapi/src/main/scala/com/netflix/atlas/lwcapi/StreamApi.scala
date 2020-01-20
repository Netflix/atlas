/*
 * Copyright 2014-2020 Netflix, Inc.
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
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

class StreamApi @Inject() (
  config: Config,
  registry: Registry,
  sm: StreamSubscriptionManager,
  splitter: ExpressionSplitter,
  implicit val materializer: Materializer
) extends WebApi
    with StrictLogging {

  import StreamApi._

  private implicit val ec = scala.concurrent.ExecutionContext.global

  private val queueSize = config.getInt("atlas.lwcapi.queue-size")

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
      val msg = DiagnosticMessage.info(s"dropped: another connection is using id: $streamId")
      queue.offer(msg)
      queue.complete()
    }

    // Create queue to allow messages coming into /evaluate to be passed to this stream
    val (queue, pub) = StreamOps
      .blockingQueue[JsonSupport](registry, "StreamApi", queueSize)
      .toMat(Sink.asPublisher[JsonSupport](true))(Keep.both)
      .run()

    // Send initial setup messages
    val handler = new QueueHandler(streamId, queue)
    queue.offer(DiagnosticMessage.info(s"setup stream $streamId on $instanceId"))
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
            LwcHeartbeat(stepAlignedTime(step), step)
          }
        Source(steps)
      }

    val source = Source
      .fromPublisher(pub)
      .merge(heartbeatSrc)
      .map(msg => ChunkStreamPart(LwcMessages.toSSE(msg)))
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

object StreamApi {

  private val instanceId = NetflixEnvironment.instanceId()
}
