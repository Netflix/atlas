/*
 * Copyright 2014-2021 Netflix, Inc.
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

import java.nio.charset.StandardCharsets
import akka.NotUsed
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.ThrottleMode
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import javax.inject.Inject
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

class SubscribeApi @Inject() (
  config: Config,
  registry: Registry,
  sm: StreamSubscriptionManager,
  splitter: ExpressionSplitter,
  implicit val materializer: Materializer
) extends WebApi
    with StrictLogging {

  import SubscribeApi._
  import com.netflix.atlas.akka.OpportunisticEC._

  private val queueSize = config.getInt("atlas.lwcapi.queue-size")
  private val batchSize = config.getInt("atlas.lwcapi.batch-size")

  private val evalsId = registry.createId("atlas.lwcapi.subscribe.count")
  private val itemsId = registry.createId("atlas.lwcapi.subscribe.itemCount")

  def routes: Route = {
    endpointPathPrefix("api" / "v1" / "subscribe") {
      path(Remaining) { streamId =>
        handleWebSocketMessages(createHandlerFlow(streamId))
      }
    } ~
    endpointPathPrefix("api" / "v2" / "subscribe") {
      path(Remaining) { streamId =>
        handleWebSocketMessages(createHandlerFlowV2(streamId))
      }
    }
  }

  /**
    * Uses text messages and sends each datapoint individually.
    */
  private def createHandlerFlow(streamId: String): Flow[Message, Message, Any] = {
    // Drop any other connections that may already be using the same id
    sm.unregister(streamId).foreach { queue =>
      val msg = DiagnosticMessage.info(s"dropped: another connection is using id: $streamId")
      queue.offer(msg)
      queue.complete()
    }

    Flow[Message]
      .flatMapConcat {
        case TextMessage.Strict(str) =>
          Source.single(str)
        case msg: TextMessage =>
          msg.textStream.fold("")(_ + _)
        case BinaryMessage.Strict(str) =>
          Source.single(str.decodeString(StandardCharsets.UTF_8))
        case msg: BinaryMessage =>
          msg.dataStream.fold(ByteString.empty)(_ ++ _).map(_.decodeString(StandardCharsets.UTF_8))
      }
      .via(new WebSocketSessionManager(streamId, register, subscribe))
      .flatMapMerge(Int.MaxValue, s => s)
      .map(obj => TextMessage(obj.toJson))
  }

  /**
    * Uses a binary format for the messages and batches output to achieve higher throughput.
    */
  private def createHandlerFlowV2(streamId: String): Flow[Message, Message, Any] = {
    // Drop any other connections that may already be using the same id
    sm.unregister(streamId).foreach { queue =>
      val msg = DiagnosticMessage.info(s"dropped: another connection is using id: $streamId")
      queue.offer(msg)
      queue.complete()
    }

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
      .via(new WebSocketSessionManager(streamId, register, subscribe))
      .flatMapMerge(Int.MaxValue, msg => msg)
      .groupedWithin(batchSize, 1.second)
      .statefulMapConcat { () =>
        // Re-use the stream to reduce allocations
        val baos = new ByteArrayOutputStream()

        { seq =>
          List(BinaryMessage(LwcMessages.encodeBatch(seq, baos)))
        }
      }
  }

  private def stepAlignedTime(step: Long): Long = {
    registry.clock().wallTime() / step * step
  }

  private def register(streamId: String): (QueueHandler, Source[JsonSupport, Unit]) = {

    // Create queue to allow messages coming into /evaluate to be passed to this stream
    val (queue, queueSrc) = StreamOps
      .blockingQueue[JsonSupport](registry, "SubscribeApi", queueSize)
      .toMat(BroadcastHub.sink(1))(Keep.both)
      .run()

    // Send initial setup messages
    queue.offer(DiagnosticMessage.info(s"setup stream $streamId on $instanceId"))
    val handler = new QueueHandler(streamId, queue)
    sm.register(streamId, handler)

    // Heartbeat messages to ensure that the socket is never idle
    val heartbeatSrc = Source
      .repeat(NotUsed)
      .throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
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

    val source = queueSrc
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

    handler -> source
  }

  private def subscribe(streamId: String, expressions: List[ExpressionMetadata]): List[ErrorMsg] = {
    registry.counter(evalsId.withTag("action", "subscribe")).increment()
    registry.counter(itemsId.withTag("action", "subscribe")).increment(expressions.size)

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
          queue.offer(LwcSubscription(expr.expression, List(exprInfo)))
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

    require(streamId != null && !streamId.isEmpty, "streamId attribute is missing or empty")
    require(
      expressions != null && expressions.nonEmpty,
      "expressions attribute is missing or empty"
    )
  }

  case class ErrorMsg(expression: String, message: String)

  case class Errors(`type`: String, message: String, errors: List[ErrorMsg]) extends JsonSupport
}
