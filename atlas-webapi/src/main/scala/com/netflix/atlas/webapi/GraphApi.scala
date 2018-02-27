/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.webapi

import akka.actor.ActorRefFactory
import akka.actor.Props
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.ImperativeRequestContext
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model._
import com.netflix.atlas.eval.graph.GraphConfig
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.spectator.api.Spectator
import com.typesafe.config.Config

class GraphApi(config: Config, implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val grapher: Grapher = Grapher(config)

  private val registry = Spectator.globalRegistry()

  def routes: Route = {
    path("api" / "v1" / "graph") {
      get { ctx =>
        val reqHandler = actorRefFactory.actorOf(Props(new GraphRequestActor(grapher, registry)))
        val graphCfg = grapher.toGraphConfig(ctx.request.uri)
        val rc = ImperativeRequestContext(graphCfg, ctx)
        reqHandler ! rc
        rc.promise.future
      }
    } ~
    path("api" / "v2" / "fetch") {
      get {
        extractRequest { request =>
          val graphCfg = grapher.toGraphConfig(request.uri)
          complete(FetchRequestActor.createResponse(actorRefFactory, graphCfg))
        }
      }
    }
  }
}

object GraphApi {

  case class DataRequest(context: EvalContext, exprs: List[DataExpr])

  object DataRequest {

    def apply(config: GraphConfig): DataRequest = {
      val dataExprs = config.exprs.flatMap(_.expr.dataExprs)
      val deduped = dataExprs.distinct
      DataRequest(config.evalContext, deduped)
    }
  }

  case class DataResponse(ts: Map[DataExpr, List[TimeSeries]])
}
