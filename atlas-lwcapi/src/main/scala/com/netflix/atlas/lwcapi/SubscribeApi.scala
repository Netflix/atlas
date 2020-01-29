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

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.ThrottleMode
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
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
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

  private implicit val ec = scala.concurrent.ExecutionContext.global

  private val queueSize = config.getInt("atlas.lwcapi.queue-size")

  private val evalsId = registry.createId("atlas.lwcapi.subscribe.count")
  private val itemsId = registry.createId("atlas.lwcapi.subscribe.itemCount")

  def routes: Route = {
    endpointPathPrefix("api" / "v1" / "subscribe") {
      path(Remaining) { streamId =>
        handleWebSocketMessages(createHandlerFlow(streamId))
      }
    } ~
    endpointPath("lwc" / "api" / "v1" / "subscribe") {
      post {
        parseEntity(json[SubscribeRequest]) {
          case SubscribeRequest(_, Nil) =>
            complete(DiagnosticMessage.error(StatusCodes.BadRequest, "empty payload"))
          case SubscribeRequest(streamId, expressions) =>
            val errors = subscribe(streamId, expressions)
            val errorResponse = if (errors.isEmpty) {
              Errors("success", "success", List())
            } else {
              Errors("error", "Some expressions could not be parsed", errors)
            }
            val entity = HttpEntity(MediaTypes.`application/json`, errorResponse.toJson)
            complete(HttpResponse(StatusCodes.OK, entity = entity))
        }
      }
    }
  }

  private def createHandlerFlow(streamId: String): Flow[Message, Message, Any] = {
    // Drop any other connections that may already be using the same id
    sm.unregister(streamId).foreach { queue =>
      val msg = DiagnosticMessage.info(s"dropped: another connection is using id: $streamId")
      queue.offer(msg)
      queue.complete()
    }

    Flow[Message]
      .flatMapConcat {
        case msg: TextMessage =>
          msg.textStream.fold("")(_ + _)
        case msg: BinaryMessage =>
          msg.dataStream.fold(ByteString.empty)(_ ++ _).map(_.decodeString(StandardCharsets.UTF_8))
      }
      .via(new WebSocketSessionManager(streamId, register, subscribe))
      .flatMapMerge(Int.MaxValue, msg => msg)
  }

  private def stepAlignedTime(step: Long): Long = {
    registry.clock().wallTime() / step * step
  }

  private def register(streamId: String): (QueueHandler, Source[Message, Unit]) = {

    // Create queue to allow messages coming into /evaluate to be passed to this stream
    val (queue, pub) = StreamOps
      .blockingQueue[JsonSupport](registry, "SubscribeApi", queueSize)
      .map(msg => TextMessage(msg.toJson))
      .toMat(Sink.asPublisher[Message](true))(Keep.both)
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
            TextMessage(LwcHeartbeat(stepAlignedTime(step), step).toJson)
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
