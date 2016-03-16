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

import akka.actor.ActorRefFactory
import com.netflix.iep.service.ServiceManager
import com.typesafe.scalalogging.StrictLogging
import spray.http._
import spray.routing._


/**
  * Healthcheck endpoint based on health status of the ServiceManager. For backwards
  * compatiblity, the status is marked as healthy if the ServiceManager is null. That
  * behavior will likely change in the future, but for now just logs a warning.
  */
class HealthcheckApi(val actorRefFactory: ActorRefFactory, serviceManager: ServiceManager)
    extends WebApi with StrictLogging {

  if (serviceManager == null) {
    logger.warn("ServiceManager is null, healthcheck will always return true")
  }

  def routes: RequestContext => Unit = {
    path("healthcheck") {
      if (serviceManager == null || serviceManager.isHealthy)
        complete(StatusCodes.OK)
      else
        complete(StatusCodes.InternalServerError)
    }
  }
}
