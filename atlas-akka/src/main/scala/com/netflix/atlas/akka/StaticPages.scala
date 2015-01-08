/*
 * Copyright 2015 Netflix, Inc.
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
import spray.http._
import spray.routing._

/**
 * Adds a directive to serve static pages from the 'www' resource directory.
 */
class StaticPages(implicit val actorRefFactory: ActorRefFactory) extends WebApi {
  def routes: RequestContext => Unit = {
    pathEndOrSingleSlash {
      redirect("/static/index.html", StatusCodes.MovedPermanently)
    } ~
    pathPrefix("static") {
      pathEndOrSingleSlash {
        getFromResource("www/index.html")
      } ~
      getFromResourceDirectory("www")
    }
  }
}
