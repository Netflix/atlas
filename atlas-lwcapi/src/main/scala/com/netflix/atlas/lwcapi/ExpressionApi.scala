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

import akka.actor.ActorRefFactory
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.{Json, JsonSupport}
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import spray.http.{HttpResponse, StatusCodes}
import spray.routing.RequestContext

case class ExpressionApi @Inject()(expressionDatabase: ExpressionDatabase,
  registry: Registry,
  implicit val actorRefFactory: ActorRefFactory)
  extends WebApi with StrictLogging {
  import ExpressionApi._

  private val expressionFetchesId = registry.createId("atlas.lwcapi.expressions.fetches")
  private val expressionCount = registry.distributionSummary("atlas.lwcapi.expressions.count")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "expressions" / Segment) { (cluster) =>
      get { ctx => handleReq(ctx, cluster) }
    }
  }

  private def handleReq(ctx: RequestContext, cluster: String): Unit = {
    val expressions = expressionDatabase.expressionsForCluster(cluster)
    val json = Return(expressions).toJson
    ctx.responder ! HttpResponse(StatusCodes.OK, entity = json)
    registry.counter(expressionFetchesId.withTag("cluster", cluster)).increment()
    expressionCount.record(expressions.size)
  }
}

object ExpressionApi {
  case class Return(expressions: List[ExpressionWithFrequency]) extends JsonSupport
}
