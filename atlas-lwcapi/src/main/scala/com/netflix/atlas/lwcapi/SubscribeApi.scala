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

class SubscribeApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  import SubscribeApi._

  private val subscribeRef = actorRefFactory.actorSelection("/user/lwc.subscribe")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "subscribe") {
      get { ctx => handleReq(ctx) }
    }
  }

  private def handleReq(ctx: RequestContext): Unit = {
    println("GOT A REQUEST")
    subscribeRef.tell(Spam(100000), ctx.responder)
  }
}

object SubscribeApi {
  case class Spam(count: Int) extends JsonSupport
}
