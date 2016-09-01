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
import com.netflix.atlas.lwcapi.AlertMap.ReturnableExpression
import spray.http.{HttpResponse, StatusCodes}
import spray.routing.RequestContext

class ExpressionsApi (implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  import ExpressionsApi._

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "expressions" / Segment) { (cluster) =>
      get { ctx => handleReq(ctx, cluster) }
    }
  }

  private def handleReq(ctx: RequestContext, cluster: String): Unit = {
    val expressions = AlertMap.globalAlertMap.expressionsForCluster(cluster)
    ctx.responder ! HttpResponse(StatusCodes.OK, entity = toJson(expressions)) // TODO fix
  }
}

object ExpressionsApi {
  private def toJson(expressions: List[ReturnableExpression]): String = {
    Json.encode(expressions)
  }
}
