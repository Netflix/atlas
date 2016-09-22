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
import com.netflix.atlas.json.JsonSupport
import com.typesafe.scalalogging.StrictLogging
import spray.routing.RequestContext

class StreamApi @Inject()(sm: SubscriptionManager,
                          splitter: ExpressionSplitter,
                          alertmap: ExpressionDatabase,
                          implicit val actorRefFactory: ActorRefFactory) extends WebApi with StrictLogging {
  import StreamApi.SSESubscribe

  private val dbActor = actorRefFactory.actorSelection("/user/lwc.expressiondb")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "stream" / Segment) { (streamId) =>
      parameters('name.?, 'expr.?, 'frequency.?) { (name, expr, frequency) =>
        get { (ctx) =>
          handleReq(ctx, streamId, name, expr, frequency)
        }
      }
    }
  }

  private def handleReq(ctx: RequestContext, streamId: String, name: Option[String], expr: Option[String], freqString: Option[String]): Unit = {
    try {
      val actorRef = actorRefFactory.actorOf(Props(new SSEActor(ctx.responder, streamId, name.getOrElse("unknown"), sm)))

      val freq = freqString.fold(ApiSettings.defaultFrequency)(_.toLong)
      if (expr.isDefined) {
        val split = splitter.split(ExpressionWithFrequency(expr.get, freq))
        dbActor ! ExpressionDatabaseActor.Expression(split)
        dbActor ! ExpressionDatabaseActor.Subscribe(streamId, split.id)
        actorRef ! SSESubscribe(split)
      }
    } catch handleException(ctx)
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
  case class HelloContent(streamId: String) extends JsonSupport

  case class SSEHello(streamId: String)
    extends SSEMessage("data", "hello", HelloContent(streamId))

  // Heartbeat message
  case class HeartbeatContent() extends JsonSupport

  case class SSEHeartbeat()
    extends SSEMessage("data", "heartbeat", HeartbeatContent())

  // Shutdown message
  case class ShutdownReason(reason: String) extends JsonSupport

  case class SSEShutdown(reason: String)
    extends SSEMessage("data", "shutdown", ShutdownReason(reason))

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
    extends SSEMessage("data", "subscribe", SubscribeContent(split))

  // Evaluate message
  case class SSEEvaluate(item: EvaluateApi.Item)
    extends SSEMessage("data", "evaluate", item)
}
