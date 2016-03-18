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
package com.netflix.atlas.akka

import javax.inject.Provider

import akka.actor.ActorRefFactory
import com.netflix.atlas.json.Json
import com.netflix.iep.service.ServiceManager
import com.typesafe.scalalogging.StrictLogging
import spray.http._
import spray.routing._


/**
  * Healthcheck endpoint based on health status of the ServiceManager.
  */
class HealthcheckApi(
    val actorRefFactory: ActorRefFactory,
    serviceManagerProvider: Provider[ServiceManager]) extends WebApi with StrictLogging {

  def routes: RequestContext => Unit = {
    serviceManagerProvider.get()
    path("healthcheck") {
      respondWithMediaType(MediaTypes.`application/json`) {
        get { ctx =>
          val status = if (serviceManager.isHealthy) StatusCodes.OK else StatusCodes.InternalServerError
          ctx.responder ! HttpResponse(status, entity = summary)
        }
      }
    }
  }

  private def serviceManager: ServiceManager = serviceManagerProvider.get

  private def summary: String = {
    import scala.collection.JavaConversions._
    val states = serviceManager.services().map(s => s.name -> s.isHealthy).toMap
    Json.encode(states)
  }
}
