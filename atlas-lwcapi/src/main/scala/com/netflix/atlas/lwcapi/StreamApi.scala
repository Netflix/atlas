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
import com.typesafe.scalalogging.StrictLogging
import spray.httpx.PlayJsonSupport
import spray.routing.RequestContext

class StreamApi @Inject()(sm: SubscriptionManager,
                          splitter: ExpressionSplitter,
                          alertmap: AlertMap,
                          implicit val actorRefFactory: ActorRefFactory) extends WebApi with StrictLogging {
  import StreamApi.SSESubscribe

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "stream" / Segment) { (sseId) =>
      parameters('name.?, 'expr.?, 'frequency.?) { (name, expr, frequency) =>
        get { (ctx) =>
          handleReq(ctx, sseId, name, expr, frequency)
        }
      }
    }
  }

  private def handleReq(ctx: RequestContext, sseId: String, name: Option[String], expr: Option[String], freqString: Option[String]): Unit = {
    val actorRef = actorRefFactory.actorOf(Props(new SSEActor(ctx.responder, sseId, name.getOrElse("unknown"), sm)))

    val freq = freqString.fold(ApiSettings.defaultFrequency)(_.toLong)
    if (expr.isDefined) {
      val split = splitter.split(ExpressionWithFrequency(expr.get, freq))
      alertmap.addExpr(split)
      sm.subscribe(sseId, split.id)
      actorRef ! SSESubscribe(split)
    }
  }
}

trait SSERenderable {
  def toSSE: String
}

object StreamApi {
  abstract class SSEMessage(msgType: String, what: String, content: JsonSupport) extends SSERenderable {
    def toSSE = s"$msgType: $what ${content.toJson}"
  }

  // Hello message
  case class HelloContent(sseId: String) extends JsonSupport

  case class SSEHello(sseId: String)
    extends SSEMessage("data", "hello", HelloContent(sseId))

  // Heartbeat message
  case class HeartbeatContent() extends JsonSupport

  case class SSEHeartbeat()
    extends SSEMessage("data", "heartbeat", HeartbeatContent()) with SSERenderable

  // Shutdown message
  case class ShutdownReason(reason: String) extends JsonSupport

  case class SSEShutdown(reason: String)
    extends SSEMessage("data", "shutdown", ShutdownReason(reason)) with SSERenderable

  // Subscribe message
  case class SubscribeContent(id: String,
                              expression: String,
                              frequency: Long,
                              dataExpressions: List[String]) extends JsonSupport

  object SubscribeContent {
    def apply(split: ExpressionSplitter.SplitResult) = {
      new SubscribeContent(split.id, split.expression, split.frequency, split.split.map(e => e.dataExpr.toString))
    }
  }

  case class SSESubscribe(split: ExpressionSplitter.SplitResult)
    extends SSEMessage("data", "subscribe", SubscribeContent(split)) with SSERenderable

  // Evaluate message
  case class SSEEvaluate(item: EvaluateApi.Item)
    extends SSEMessage("data", "evaluate", item) with SSERenderable
}
