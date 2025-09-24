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
package com.netflix.atlas.lwcapi

import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

/**
  * Service that will report as unhealthy for a configured window after startup. This is
  * used to allow clients that will stream data time to connect before marking the instance
  * as UP so that clients publishing data will send to the instance.
  */
class StartupDelayService(registry: Registry, config: Config)
    extends AbstractService
    with StrictLogging {

  private val clock = registry.clock()
  private val delay = config.getDuration("atlas.lwcapi.startup-delay").toMillis

  // Timestamp for when to switch to being healthy
  @volatile private var switchTimestamp = -1L

  override def isHealthy: Boolean = {
    val timeRemaining = switchTimestamp - clock.wallTime()
    if (switchTimestamp == -1L) {
      logger.debug("service has not been started")
    } else if (timeRemaining > 0L) {
      logger.debug(s"waiting for another $timeRemaining milliseconds to report healthy")
    }
    switchTimestamp > 0L && timeRemaining <= 0L
  }

  override def startImpl(): Unit = {
    switchTimestamp = clock.wallTime() + delay
  }

  override def stopImpl(): Unit = {}
}
