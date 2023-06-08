/*
 * Copyright 2014-2023 Netflix, Inc.
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
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.CustomDirectives.*
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.config.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import scala.concurrent.duration.*
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

class SubscribeApi(
  config: Config,
  registry: Registry,
  sm: StreamSubscriptionManager,
  splitter: ExpressionSplitter,
  implicit val materializer: Materializer
) extends WebApi
    with StrictLogging {

  import SubscribeApi.*
  import com.netflix.atlas.akka.OpportunisticEC.*

  private val queueSize = config.getInt("atlas.lwcapi.queue-size")
  private val batchSize = config.getInt("atlas.lwcapi.batch-size")

  private val evalsCounter = registry.counter("atlas.lwcapi.subscribe.count", "action", "subscribe")

  private val itemsCounter =
    registry.counter("atlas.lwcapi.subscribe.itemCount", "action", "subscribe")

  def routes: Route = {
    extractClientIP { addr =>
      endpointPathPrefix("api" / "v2" / "subscribe") {
        path(Remaining) { streamId =>
          val meta = StreamMetadata(streamId, addr.value)
          handleWebSocketMessages(createHandlerFlowV2(meta))
        }
      }
    }
  }

  /**
    * Drop any other connections that may already be using the same id
    */
  private def dropSameIdConnections(streamMeta: StreamMetadata): Unit = {
    val streamId = streamMeta.streamId
    sm.unregister(streamId).foreach { queue =>
      val msg = DiagnosticMessage.info(s"dropped: another connection is using id: $streamId")
      queue.offer(Seq(msg))
      queue.complete()
    }
  }

  /**
    * Uses a binary format for the messages and batches output to achieve higher throughput.
    */
  private def createHandlerFlowV2(streamMeta: StreamMetadata): Flow[Message, Message, Any] = {
    dropSameIdConnections(streamMeta)

    Flow[Message]
      .flatMapConcat {
        case msg: TextMessage =>
          // Text messages are not supported, ignore
          msg.textStream.runWith(Sink.ignore)
          Source.empty
        case BinaryMessage.Strict(str) =>
          Source.single(str)
        case msg: BinaryMessage =>
          msg.dataStream.fold(ByteString.empty)(_ ++ _)
      }
      .via(new WebSocketSessionManager(streamMeta, register, subscribe))
      .flatMapMerge(Int.MaxValue, msg => msg)
      .groupedWithin(batchSize, 1.second)
      .statefulMapConcat { () =>
        // Re-use the stream to reduce allocations
        val baos = new ByteArrayOutputStream()

        { seq =>
          List(BinaryMessage(LwcMessages.encodeBatch(seq, baos)))
        }
      }
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) =>
            logger.debug(s"lost client for $streamMeta.streamId")
            sm.unregister(streamMeta.streamId)
          case Failure(t) =>
            logger.debug(s"lost client for $streamMeta.streamId", t)
            sm.unregister(streamMeta.streamId)
        }
      }
  }

  private def stepAlignedTime(step: Long): Long = {
    registry.clock().wallTime() / step * step
  }

  private def register(streamMeta: StreamMetadata): (QueueHandler, Source[JsonSupport, NotUsed]) = {

    val streamId = streamMeta.streamId

    // Create queue to allow messages coming into /evaluate to be passed to this stream
    // TODO - A client can connect but not send a message. When that happens, the
    // publisher sink from this queue will shutdown and complete the queue. See
    // akka.http.client.stream-cancellation-delay. Unfortunately
    // the websocket flow is not notified of the shutdown. If the client sends a message
    // after shutdown, the client flow will be terminated. (that's fine).
    // There is likely another way to wire this up. Alternatively we could hold a
    // kill switch on the createHandlerFlow...()s but some state flags are needed and
    // it gets messy.
    // For now, the queue will close and if no messages are sent from the client, the
    // akka.http.server.idle-timeout will kill the client connection and we'll try to
    // close a closed queue.
    val (queue, pub) = StreamOps
      .blockingQueue[Seq[JsonSupport]](registry, "SubscribeApi", queueSize)
      .toMat(Sink.asPublisher(true))(Keep.both)
      .run()

    // Send initial setup messages
    queue.offer(Seq(DiagnosticMessage.info(s"setup stream $streamId on $instanceId")))
    val handler = new QueueHandler(streamMeta, queue)
    sm.register(streamMeta, handler)

    // Heartbeat messages to ensure that the socket is never idle
    val heartbeatSrc = Source
      .tick(0.seconds, 5.seconds, NotUsed)
      .flatMapConcat { _ =>
        val steps = sm
          .subscriptionsForStream(streamId)
          .map(_.metadata.frequency)
          .distinct
          .map { step =>
            // To account for some delays for data coming from real systems, the heartbeat
            // timestamp is delayed by one interval
            LwcHeartbeat(stepAlignedTime(step) - step, step)
          }
        Source(steps)
      }

    val source = Source
      .fromPublisher(pub)
      .flatMapConcat(Source.apply)
      .merge(heartbeatSrc)
      .viaMat(StreamOps.monitorFlow(registry, "StreamApi"))(Keep.left)

    handler -> source
  }

  private def subscribe(streamId: String, expressions: List[ExpressionMetadata]): List[ErrorMsg] = {
    evalsCounter.increment()
    itemsCounter.increment(expressions.size)

    val errors = scala.collection.mutable.ListBuffer[ErrorMsg]()
    val subIdsBuilder = Set.newBuilder[String]

    expressions.foreach { expr =>
      try {
        val splits = splitter.split(expr.expression, expr.frequency)

        // Add any new expressions
        val (queue, addedSubs) = sm.subscribe(streamId, splits)
        addedSubs.foreach { sub =>
          val meta = sub.metadata
          val exprInfo = LwcDataExpr(meta.id, meta.expression, meta.frequency)
          queue.offer(Seq(LwcSubscription(expr.expression, List(exprInfo))))
        }

        // Add expression ids in use by this split
        subIdsBuilder ++= splits.map(_.metadata.id)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Unable to subscribe to expression ${expr.expression}", e)
          errors += ErrorMsg(expr.expression, e.getMessage)
      }
    }

    // Remove any expressions that are no longer required
    val subIds = subIdsBuilder.result()
    sm.subscriptionsForStream(streamId)
      .filter(s => !subIds.contains(s.metadata.id))
      .foreach(s => sm.unsubscribe(streamId, s.metadata.id))

    errors.toList
  }
}

object SubscribeApi {

  private val instanceId = NetflixEnvironment.instanceId()

  case class SubscribeRequest(streamId: String, expressions: List[ExpressionMetadata])
      extends JsonSupport {

    require(streamId != null && streamId.nonEmpty, "streamId attribute is missing or empty")

    require(
      expressions != null && expressions.nonEmpty,
      "expressions attribute is missing or empty"
    )
  }

  case class ErrorMsg(expression: String, message: String)

  case class Errors(`type`: String, message: String, errors: List[ErrorMsg]) extends JsonSupport
}
