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
package com.netflix.atlas.akka

import javax.inject.Provider

import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.json.Json
import com.netflix.iep.service.ServiceManager
import com.typesafe.scalalogging.StrictLogging

/**
  * Healthcheck endpoint based on health status of the ServiceManager.
  */
class HealthcheckApi(serviceManagerProvider: Provider[ServiceManager])
    extends WebApi
    with StrictLogging {

  def routes: Route = {
    endpointPath("healthcheck") {
      get {
        val status =
          if (serviceManager.isHealthy) StatusCodes.OK else StatusCodes.InternalServerError
        val entity = HttpEntity(MediaTypes.`application/json`, summary)
        complete(HttpResponse(status = status, entity = entity))
      }
    }
  }

  private def serviceManager: ServiceManager = serviceManagerProvider.get

  private def summary: String = {
    import scala.collection.JavaConverters._
    val states = serviceManager.services().asScala.map(s => s.name -> s.isHealthy).toMap
    Json.encode(states)
  }
}
