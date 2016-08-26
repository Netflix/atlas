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
import com.netflix.atlas.json.Json
import spray.routing.RequestContext

import scala.util.control.NonFatal

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
      val request = RegisterRequest(ctx.request.entity.asString)
      registerRef.tell(request, ctx.responder)
    } catch handleException(ctx)
  }

  private def handleDelete(ctx: RequestContext): Unit = {
    try {
      val request = DeleteRequest(ctx.request.entity.asString)
      registerRef.tell(request, ctx.responder)
    } catch handleException(ctx)
  }
}

object RegisterApi {
  case class RegisterRequest(expressions: List[ExpressionWithFrequency]) {
    def toJson = { Json.encode(this) }
  }

  object RegisterRequest {
    def apply(json: String): RegisterRequest = {
      val decoded = try {
        Json.decode[RegisterRequest](json)
      } catch {
        case NonFatal(t) => throw new IllegalArgumentException("improperly formatted request body")
      }
      if (decoded.expressions == null || decoded.expressions.isEmpty)
        throw new IllegalArgumentException("Missing or empty expressions array")
      decoded
    }
  }

  case class DeleteRequest(expressions: List[ExpressionWithFrequency]) {
    def toJson = { Json.encode(this) }
  }

  object DeleteRequest {
    def apply(json: String): DeleteRequest = {
      try {
        Json.decode[DeleteRequest](json)
      } catch {
        case NonFatal(t) => throw new IllegalArgumentException("improperly formatted request body")
      }
    }
  }
}
