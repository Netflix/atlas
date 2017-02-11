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
package com.netflix.atlas.standalone

import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton

import akka.actor.Props
import com.netflix.atlas.akka.WebServer
import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.atlas.webapi.ApiSettings
import com.netflix.atlas.webapi.LocalDatabaseActor
import com.netflix.atlas.webapi.LocalPublishActor
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config

@Singleton
class WebServerProvider @Inject() (config: Config, classFactory: ClassFactory, registry: Registry)
    extends Provider[WebServer] {

  override def get(): WebServer = null
  /*
    new WebServer(config, classFactory, registry) {
      override protected def configure(): Unit = {
        super.configure()
        val db = ApiSettings.newDbInstance
        actorSystem.actorOf(Props(new LocalDatabaseActor(db)), "db")
        db match {
          case mem: MemoryDatabase =>
            logger.info("enabling local publish to memory database")
            actorSystem.actorOf(Props(new LocalPublishActor(registry, mem)), "publish")
          case _ =>
        }
      }
    }
  }*/
}
