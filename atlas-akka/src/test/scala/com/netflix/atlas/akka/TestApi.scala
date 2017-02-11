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
package com.netflix.atlas.akka

import akka.actor.ActorRefFactory
import akka.actor.Props
import com.netflix.atlas.json.Json
import spray.http._
import spray.routing._


class TestApi(val actorRefFactory: ActorRefFactory) extends WebApi {

  import CustomDirectives._
  import spray.http.StatusCodes._

  def routes: RequestContext => Unit = {
    path("jsonparse") {
      post { ctx =>
        val parser = getJsonParser(ctx.request).get
        try {
          val v = Json.decode[String](parser)
          ctx.responder ! HttpResponse(status = OK, entity = v)
        } finally {
          parser.close()
        }
      }
    } ~
    accessLog {
      path("chunked") {
        get { ctx =>
          val ref = actorRefFactory.actorOf(Props(new ChunkResponseActor))
          ref.tell("start", ctx.responder)
        }
      }
    }
  }
}
