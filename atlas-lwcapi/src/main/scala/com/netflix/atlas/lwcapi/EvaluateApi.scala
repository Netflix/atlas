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

import akka.actor.ActorRefFactory
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.json.Json
import spray.routing.RequestContext

import scala.util.control.NonFatal

class EvaluateApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  import EvaluateApi._

  private val evaluateRef = actorRefFactory.actorSelection("/user/lwc.evaluate")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "evaluate") {
       post { ctx => handleReq(ctx) }
    }
  }

  private def handleReq(ctx: RequestContext): Unit = {
    try {
      val request = EvaluateRequest(ctx.request.entity.asString)
      evaluateRef.tell(request, ctx.responder)
    } catch handleException(ctx)
  }
}

object EvaluateApi {
  type TagMap = Map[String, String]
  case class DataExpression(tags: TagMap, value: Double)
  case class Item(timestamp: Long, id: String, dataExpressions: List[DataExpression])

  case class EvaluateRequest(items: List[Item]) {
    def toJson = { Json.encode(this) }
  }

  object EvaluateRequest {
    def apply(json: String): EvaluateRequest = {
      val decoded = try {
        Json.decode[List[Item]](json)
      } catch {
        case NonFatal(t) => throw new IllegalArgumentException("improperly formatted request body")
      }
      EvaluateRequest(decoded)
    }
  }

  case class ErrorResponse(failureCount: Long, failed: Map[String, String]) {
    def toJson = {Json.encode(this) }
  }

  object ErrorResponse {
    def apply(json: String): ErrorResponse = {
      val decoded = try {
        Json.decode[ErrorResponse](json)
      } catch {
        case NonFatal(t) => throw new IllegalArgumentException("improperly formatted request body")
      }
      decoded
    }
  }
}
