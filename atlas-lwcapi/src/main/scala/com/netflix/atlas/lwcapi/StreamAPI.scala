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
import com.netflix.atlas.lwcapi.SSEApi.{SSEMessage, SSESubscribe}
import com.typesafe.scalalogging.StrictLogging
import spray.httpx.PlayJsonSupport
import spray.routing.RequestContext

class SSEApi @Inject() (sm: SubscriptionManager,
                        splitter: ExpressionSplitter,
                        alertmap: AlertMap,
                        implicit val actorRefFactory: ActorRefFactory) extends WebApi with StrictLogging {

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
    val newActor = actorRefFactory.actorOf(Props(new SSEActor(ctx.responder, sseId, sm)))
    sm.register(sseId, newActor, name.getOrElse("unknown"))

    val freq = freqString.fold(ApiSettings.defaultFrequency)(_.toLong)
    if (expr.isDefined) {
      val split = splitter.split(ExpressionWithFrequency(expr.get, freq))
      alertmap.addExpr(split)
      sm.subscribe(sseId, split.id)
      newActor ! SSESubscribe(split)
    }
  }
}

object SSEApi {
  abstract class SSEMessage(msgType: String, what: String, content: JsonSupport) extends JsonSupport

  case class HeartbeatContent() extends JsonSupport
  case class SSEHeartbeat() extends SSEMessage("data", "heartbeat", HeartbeatContent())

  case class MessageReason(reason: String) extends JsonSupport
  case class SSEShutdown(reason: String) extends SSEMessage("data", "shutdown", MessageReason(reason))

  case class SubscribeContent(id: String, expression: String, frequency: Long, dataExpressions: List[String]) extends JsonSupport

  object SubscribeContent {
    def apply(split: ExpressionSplitter.SplitResult) = {
      new SubscribeContent(split.id, split.expression, split.frequency, split.split.map(e => e.dataExpr.toString))
    }
  }
  case class SSESubscribe(split: ExpressionSplitter.SplitResult) extends SSEMessage("data", "subscribe", SubscribeContent(split))

  case class HelloContent(sseId: String) extends JsonSupport

  case class SSEHello(sseId: String) extends SSEMessage("data", "hello", HelloContent(sseId))

  case class SSEExpression(item: EvaluateApi.Item) extends SSEMessage("data", "evaluate", item)
}
