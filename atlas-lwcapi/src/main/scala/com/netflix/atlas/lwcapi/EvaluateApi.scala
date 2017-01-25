/*
 * Copyright 2014-2017 Netflix, Inc.
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
import com.netflix.atlas.json.JsonSupport
import spray.routing.RequestContext

class EvaluateApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  import EvaluateApi._

  private val evaluateRef = actorRefFactory.actorSelection("/user/lwc.evaluate")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "evaluate") {
       post { ctx => handleReq(ctx) }
    }
  }

  private def handleReq(ctx: RequestContext): Unit = {
    val request = EvaluateRequest.fromJson(ctx.request.entity.asString)
    evaluateRef.tell(request, ctx.responder)
  }
}

object EvaluateApi {
  type TagMap = Map[String, String]

  case class Item(id: String, tags: TagMap, value: Double) extends JsonSupport

  case class EvaluateRequest(timestamp: Long, metrics: List[Item]) extends JsonSupport
  object EvaluateRequest {
    def fromJson(json: String) = Json.decode[EvaluateRequest](json)
  }
}
