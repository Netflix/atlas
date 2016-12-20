/*
 * Copyright 2014-2016 Netflix, Inc.
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

import javax.inject.Inject

import akka.actor.{ActorRefFactory, Props}
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.{Json, JsonSupport}
import com.netflix.atlas.lwcapi.ExpressionSplitter.SplitResult
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import spray.routing.RequestContext

class StreamApi @Inject()(sm: SubscriptionManager,
                          splitter: ExpressionSplitter,
                          implicit val actorRefFactory: ActorRefFactory,
                          registry: Registry) extends WebApi with StrictLogging {
  import StreamApi._

  private val dbActor = actorRefFactory.actorSelection("/user/lwc.expressiondb")
  private val subscribeRef = actorRefFactory.actorSelection("/user/lwc.subscribe")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "stream" / Segment) { (streamId) =>
      parameters('name.?, 'expression.?, 'frequency.?) { (name, expr, frequency) =>
        get { (ctx) => handleReq(ctx, streamId, name, expr, frequency) } ~
        post { (ctx) => handleReq(ctx, streamId, name, expr, frequency) }
      }
    }
  }

  private def splitRequest(json: String, urlExpr: Option[String], urlFreq: Option[String])
    : Map[ExpressionWithFrequency, SplitResult] = {
    val builder = Map.newBuilder[ExpressionWithFrequency, SplitResult]

    val freq = urlFreq.fold(ApiSettings.defaultFrequency)(_.toInt)
    if (urlExpr.isDefined) {
      builder += (ExpressionWithFrequency(urlExpr.get, freq) -> splitter.split(urlExpr.get, freq))
    }

    if (json.nonEmpty) {
      val request = ExpressionsRequest.fromJson(json)
      request.expressions.foreach { expr =>
        builder += (expr -> splitter.split(expr.expression, expr.frequency))
      }
    }

    builder.result
  }

  private def handleReq(ctx: RequestContext, streamId: String, name: Option[String],
    expr: Option[String], freqString: Option[String]): Unit = {
    try {
      val existingActor = sm.registration(streamId)
      if (existingActor.isDefined) {
        sm.unregister(streamId)
        existingActor.get.actorRef ! SSEShutdown(s"Dropped: another connection is using the same stream-id: $streamId", unsub = false)
      }

      // Validate post data.  This is done before creating an actor, since
      // creating the actor sends a chunked response, masking any expression
      // parse errors.
      val splits = splitRequest(ctx.request.entity.asString, expr, freqString)

      val actorRef = actorRefFactory.actorOf(Props(
        new SSEActor(ctx.responder, streamId, name.getOrElse("unknown"), sm, registry)))

      // handle post and URL subscription data
      splits.foreach { case (e, split) =>
        dbActor ! ExpressionDatabaseActor.Expression(split)
        split.expressions.foreach(e =>
          dbActor ! ExpressionDatabaseActor.Subscribe(streamId, e.id)
        )
        actorRef ! SSESubscribe(e.expression, split.expressions)
      }
    } catch handleException(ctx)
  }
}

trait SSERenderable {
  def toSSE: String
}

object StreamApi {
  case class ExpressionsRequest(expressions: List[ExpressionWithFrequency]) extends JsonSupport

  object ExpressionsRequest {
    def fromJson(json: String): ExpressionsRequest = {
      val decoded = Json.decode[ExpressionsRequest](json)
      if (decoded.expressions == null || decoded.expressions.isEmpty)
        throw new IllegalArgumentException("Missing or empty expressions array")
      decoded
    }
  }

  abstract class SSEMessage(msgType: String, what: String, content: JsonSupport) extends SSERenderable {
    def toSSE = s"$msgType: $what ${content.toJson}"
    def getWhat = what
  }

  // Hello message
  case class HelloContent(streamId: String, instanceId: String, instanceUUID: String) extends JsonSupport
  case class SSEHello(streamId: String, instanceId: String, instanceUUID: String)
    extends SSEMessage("info", "hello", HelloContent(streamId, instanceId, instanceUUID))

  // Generic message string
  case class SSEGenericJson(what: String, msg: JsonSupport) extends SSEMessage("data", what, msg)

  // Heartbeat message
  case class StatisticsContent(outstandingCount: Long, outputFullFailures: Long, maxOutstanding: Long) extends JsonSupport
  case class SSEStatistics(outstandingCount: Long, outputFullFailures: Long, maxOutstanding: Long)
    extends SSEMessage("info", "statistics", StatisticsContent(outstandingCount, outputFullFailures, maxOutstanding))

  // Shutdown message
  case class ShutdownReason(reason: String) extends JsonSupport
  case class SSEShutdown(reason: String, private val unsub: Boolean = true)
    extends SSEMessage("info", "shutdown", ShutdownReason(reason)) {
    def shouldUnregister: Boolean = unsub
  }

  // Subscribe message
  case class SubscribeContent(expression: String,
                              metrics: List[ExpressionWithFrequency]) extends JsonSupport

  case class SSESubscribe(expr: String, metrics: List[ExpressionWithFrequency])
    extends SSEMessage("info", "subscribe", SubscribeContent(expr, metrics))

  case class SSEMetricContent(timestamp: Long, id: String, tags: EvaluateApi.TagMap, value: Double) extends JsonSupport

  // Evaluate message
  case class SSEMetric(timestamp: Long, data: EvaluateApi.Item)
    extends SSEMessage("data", "metric", SSEMetricContent(timestamp, data.id, data.tags, data.value))
}
