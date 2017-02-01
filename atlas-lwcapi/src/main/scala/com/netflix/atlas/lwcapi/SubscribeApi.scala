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
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.ImperativeRequestContext
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.JsonSupport

class SubscribeApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  import SubscribeApi._

  private val subscribeRef = actorRefFactory.actorSelection("/user/lwc.subscribe")

  def routes: Route = {
    path("lwc" / "api" / "v1" / "subscribe") {
      post {
        extractRequestContext { ctx =>
          parseEntity(json[SubscribeRequest]) { req =>
            val rc = ImperativeRequestContext(req, ctx)
            subscribeRef ! rc
            _ => rc.promise.future
          }
        }
      }
    }
  }
}

object SubscribeApi {
  case class SubscribeRequest(
    streamId: String,
    expressions: List[ExpressionWithFrequency]) extends JsonSupport {

    require(streamId != null && !streamId.isEmpty, "streamId attribute is missing or empty")
    require(expressions != null && expressions.nonEmpty, "expressions attribute is missing or empty")
  }
}
