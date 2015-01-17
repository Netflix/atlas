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
package com.netflix.atlas.webapi

import java.io.File

import akka.actor.Props
import com.netflix.atlas.akka.WebServer
import com.netflix.atlas.config.ConfigManager
import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.iep.jmxport.JmxPort
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

/**
 * Provides a simple way to start up a standalone server. Usage:
 *
 * ```
 * $ java -jar atlas.jar config1.conf config2.conf
 * ```
 */
object Main extends StrictLogging {

  var server: WebServer = _

  private def loadAdditionalConfigFiles(files: Array[String]): Unit = {
    files.foreach { f =>
      logger.info(s"loading config file: $f")
      val c = ConfigFactory.parseFileAnySyntax(new File(f))
      ConfigManager.update(c)
    }
  }

  private def restrictJmxPort(): Unit = {
    if (ApiSettings.shouldRestrictJmxPort) {
      JmxPort.configure(null, ApiSettings.jmxPort)
    }
  }

  private def startServer(): Unit = {
    server = new WebServer("atlas") {
      override protected def configure(): Unit = {
        val db = ApiSettings.newDbInstance
        actorSystem.actorOf(Props(new LocalDatabaseActor(db)), "db")
        db match {
          case mem: MemoryDatabase =>
            logger.info("enabling local publish to memory database")
            actorSystem.actorOf(Props(new LocalPublishActor(mem)), "publish")
          case _ =>
        }
      }
    }
    server.start(ApiSettings.port)
  }

  def main(args: Array[String]) {
    loadAdditionalConfigFiles(args)
    restrictJmxPort()
    startServer()
  }

  def shutdown(): Unit = {
    server.shutdown()
  }
}
