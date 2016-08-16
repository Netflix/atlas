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

class EvaluateApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  import EvaluateApi._

  private val registerRef = actorRefFactory.actorSelection("/user/lwc.register")

  def routes: RequestContext => Unit = {
    post {
      path("lwc" / "api" / "v1" / "evaluate") { ctx => {
        handleReq(ctx)
      }}
    }
  }

  private def handleReq(ctx: RequestContext): Unit = {
    try {
      val request = toRequest(ctx.request.entity.asString)
      registerRef.tell(request, ctx.responder)
    } catch handleException(ctx)
  }
}

object EvaluateApi {
  case class EvaluateRequest(expressions: List[ExpressionWithFrequency], data: String)

  def toRequest(request: String): EvaluateRequest = {
    try {
      Json.decode[EvaluateRequest](request)
    } catch {
      case NonFatal(t) => throw new IllegalArgumentException("improperly formatted request body")
    }
  }

  def toJson(request: EvaluateRequest) = {
    Json.encode(request.expressions)
  }
}
