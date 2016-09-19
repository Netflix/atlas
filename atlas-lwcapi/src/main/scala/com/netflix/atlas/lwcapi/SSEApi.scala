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

import akka.actor.{ActorRefFactory, Props}
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.JsonSupport
import spray.routing.RequestContext

class SSEApi @Inject() (sm: SubscriptionManager, implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "sse" / Segment) { (sseId) =>
      get { ctx => handleReq(ctx, sseId) }
    }
  }

  private def handleReq(ctx: RequestContext, sseId: String): Unit = {
    val newActor = actorRefFactory.actorOf(Props(new SSEActor(ctx.responder, sseId, sm)), name = "foo")
  }
}

object SSEApi {
  case class SSEMessage(msgType: String, what: String, content: String)
  case class SSEShutdown(reason: String) extends JsonSupport
}
