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
package com.netflix.atlas.lwcapi

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.ws.BinaryMessage
import org.apache.pekko.http.scaladsl.model.ws.Message
import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscriptionV2
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.pekko.WebApi
import com.netflix.iep.config.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import java.util.concurrent.ArrayBlockingQueue
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
  import com.netflix.atlas.pekko.OpportunisticEC.*

  private val queueSize = config.getInt("atlas.lwcapi.queue-size")
  private val batchSize = config.getInt("atlas.lwcapi.batch-size")

  private val dropNew = config.getBoolean("atlas.lwcapi.drop-new")

  private val subscriptionSize = registry.distributionSummary("atlas.lwcapi.subscriptionSize")

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
      .statefulMap(() => new ByteArrayOutputStream())(
        (baos, seq) => baos -> BinaryMessage(LwcMessages.encodeBatch(seq, baos)),
        _ => None
      )
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

  private def register(streamMeta: StreamMetadata): Source[JsonSupport, NotUsed] = {

    val streamId = streamMeta.streamId

    // Create queue to allow messages coming into /evaluate to be passed to this stream
    // TODO - A client can connect but not send a message. When that happens, the
    // publisher sink from this queue will shutdown and complete the queue. See
    // pekko.http.client.stream-cancellation-delay. Unfortunately
    // the websocket flow is not notified of the shutdown. If the client sends a message
    // after shutdown, the client flow will be terminated. (that's fine).
    // There is likely another way to wire this up. Alternatively we could hold a
    // kill switch on the createHandlerFlow...()s but some state flags are needed and
    // it gets messy.
    // For now, the queue will close and if no messages are sent from the client, the
    // pekko.http.server.idle-timeout will kill the client connection and we'll try to
    // close a closed queue.
    val blockingQueue = new ArrayBlockingQueue[Seq[JsonSupport]](queueSize)
    val (queue, pub) = StreamOps
      .wrapBlockingQueue[Seq[JsonSupport]](registry, "SubscribeApi", blockingQueue, dropNew)
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
          .map { sub =>
            // For events where step doesn't really matter use 5s as that is the typical heartbeat
            // frequency. This only gets used for the time associated with the heartbeat messages.
            if (sub.metadata.frequency == 0L) 5_000L else sub.metadata.frequency
          }
          .distinct
          .map { step =>
            // To account for some delays for data coming from real systems, the heartbeat
            // timestamp is delayed by one interval
            LwcHeartbeat(stepAlignedTime(step) - step, step)
          }
        Source(steps)
      }

    Source
      .fromPublisher(pub)
      .flatMapConcat(Source.apply)
      .merge(heartbeatSrc)
      .viaMat(StreamOps.monitorFlow(registry, "StreamApi"))(Keep.left)
  }

  private def subscribe(
    streamId: String,
    expressions: List[ExpressionMetadata]
  ): List[JsonSupport] = {
    subscriptionSize.record(expressions.size)

    val messages = List.newBuilder[JsonSupport]
    val subIdsBuilder = Set.newBuilder[String]

    expressions.foreach { expr =>
      try {
        val splits = splitter.split(expr.expression, expr.exprType, expr.frequency)

        // Add any new expressions
        val (_, addedSubs) = sm.subscribe(streamId, splits)
        val subMessages = addedSubs.map { sub =>
          val meta = sub.metadata
          val exprInfo = LwcDataExpr(meta.id, meta.expression, meta.frequency)
          LwcSubscriptionV2(expr.expression, expr.exprType, List(exprInfo))
        }
        messages ++= subMessages

        // Add expression ids in use by this split
        subIdsBuilder ++= splits.map(_.metadata.id)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Unable to subscribe to expression ${expr.expression}", e)
          messages += DiagnosticMessage.error(s"[${expr.expression}] ${e.getMessage}")
      }
    }

    // Remove any expressions that are no longer required
    val subIds = subIdsBuilder.result()
    val unSubIds = sm
      .subscriptionsForStream(streamId)
      .filter(s => !subIds.contains(s.metadata.id))
      .map(_.metadata.id)
    sm.unsubscribe(streamId, unSubIds)

    messages.result()
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
