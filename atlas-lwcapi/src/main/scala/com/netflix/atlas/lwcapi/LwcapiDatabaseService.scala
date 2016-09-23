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

import javax.inject.Singleton

import com.netflix.iep.service.{AbstractService, ClassFactory, State}
import com.typesafe.scalalogging.StrictLogging

@Singleton
class LwcapiDatabaseService extends AbstractService with StrictLogging {
  @volatile private var dbStarted = false

  def setDbState(state: Boolean): Unit = dbStarted = state

  override def isHealthy: Boolean = state == State.RUNNING && dbStarted

  override def startImpl(): Unit = {
    logger.info("Starting LwcapiDatabaseService service monitor")
  }

  override def stopImpl(): Unit = {
    logger.info("Stopping LwcapiDatabaseService service monitor")
  }
}
