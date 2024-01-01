/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.pekko

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
  * A wrapper used for simple unit testing of Pekko HTTP calls.
  */
trait PekkoHttpClient {

  /**
    * See org.apache.pekko.http.scaladsl.Http
    */
  def singleRequest(request: HttpRequest): Future[HttpResponse]

}

/**
  * Default HTTP client wrapper that includes access logging.
  *
  * @param name
  *     The name to use for access logging and metrics.
  * @param system
  *     The actor system.
  */
class DefaultPekkoHttpClient(name: String)(implicit val system: ActorSystem)
    extends PekkoHttpClient {

  private implicit val ec: ExecutionContext = system.dispatcher

  override def singleRequest(request: HttpRequest): Future[HttpResponse] = {
    val accessLogger = AccessLogger.newClientLogger(name, request)
    Http()(system).singleRequest(request).andThen { case t => accessLogger.complete(t) }
  }
}
