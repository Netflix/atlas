/*
 * Copyright 2014-2025 Netflix, Inc.
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

import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import CustomDirectives.*
import com.netflix.atlas.json.Json
import com.netflix.iep.service.ServiceManager
import com.typesafe.scalalogging.StrictLogging

import java.util.function.Supplier

/**
  * Healthcheck endpoint based on health status of the ServiceManager.
  */
class HealthcheckApi(serviceManagerSupplier: Supplier[ServiceManager])
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

  private def serviceManager: ServiceManager = serviceManagerSupplier.get

  private def summary: String = {
    import scala.jdk.CollectionConverters.*
    val states = serviceManager.services().asScala.map(s => s.name -> s.isHealthy).toMap
    Json.encode(states)
  }
}
