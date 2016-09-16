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
import com.netflix.atlas.json.{Json, JsonSupport}
import spray.routing.RequestContext

class RegisterApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  import RegisterApi._

  private val registerRef = actorRefFactory.actorSelection("/user/lwc.register")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "register") {
      post { ctx => handlePost(ctx) } ~
      delete { ctx => handleDelete(ctx) }
    }
  }

  private def handlePost(ctx: RequestContext): Unit = {
    try {
      val request = RegisterRequest.fromJson(ctx.request.entity.asString)
      registerRef.tell(request, ctx.responder)
    } catch handleException(ctx)
  }

  private def handleDelete(ctx: RequestContext): Unit = {
    try {
      val request = DeleteRequest.fromJson(ctx.request.entity.asString)
      registerRef.tell(request, ctx.responder)
    } catch handleException(ctx)
  }
}

object RegisterApi {
  case class RegisterRequest(sinkId: Option[String], expressions: List[ExpressionWithFrequency]) extends JsonSupport

  object RegisterRequest {
    def fromJson(json: String): RegisterRequest = {
      val decoded = Json.decode[RegisterRequest](json)
      if (decoded.expressions == null || decoded.expressions.isEmpty)
        throw new IllegalArgumentException("Missing or empty expressions array")
      decoded
    }
  }

  case class DeleteRequest(sinkId: Option[String], expressions: List[ExpressionWithFrequency]) extends JsonSupport

  object DeleteRequest {
    def fromJson(json: String): DeleteRequest = Json.decode[DeleteRequest](json)
  }


  case class ErrorResponse(failureCount: Long, failed: Map[String, String]) extends JsonSupport

  object ErrorResponse {
    def fromJson(json: String): ErrorResponse = Json.decode[ErrorResponse](json)
  }
}
