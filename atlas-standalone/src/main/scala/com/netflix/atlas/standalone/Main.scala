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
package com.netflix.atlas.standalone

import java.io.File

import com.google.inject.AbstractModule
import com.google.inject.Module
import com.google.inject.multibindings.Multibinder
import com.google.inject.multibindings.OptionalBinder
import com.google.inject.util.Modules
import com.netflix.atlas.config.ConfigManager
import com.netflix.iep.guice.GuiceHelper
import com.netflix.iep.service.Service
import com.netflix.iep.service.ServiceManager
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.log4j.SpectatorAppender
import com.typesafe.config.Config
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

  private var guice: GuiceHelper = _

  private def loadAdditionalConfigFiles(files: Array[String]): Unit = {
    files.foreach { f =>
      logger.info(s"loading config file: $f")
      val c = ConfigFactory.parseFileAnySyntax(new File(f))
      ConfigManager.update(c)
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      val registry = Spectator.globalRegistry()
      SpectatorAppender.addToRootLogger(registry, "spectator", false)
      loadAdditionalConfigFiles(args)
      start()
      guice.addShutdownHook()
    } catch {
      case t: Throwable =>
        logger.error("server failed to start, shutting down", t)
        System.exit(1)
    }
  }

  def start(): Unit = {
    val serviceModule = new AbstractModule {
      override def configure(): Unit = {
        GuiceHelper.getModulesUsingServiceLoader.forEach(install)

        OptionalBinder.newOptionalBinder(binder(), classOf[Registry])
          .setBinding().toInstance(Spectator.globalRegistry())

        val serviceBinder = Multibinder.newSetBinder(binder, classOf[Service])
        serviceBinder.addBinding().toProvider(classOf[WebServerProvider])
      }
    }

    val overrides = Modules.`override`(serviceModule).`with`(new AbstractModule {
      override def configure(): Unit = {
        bind(classOf[Config]).toInstance(ConfigManager.current)
      }
    })

    val modules = new java.util.ArrayList[Module]()
    modules.add(overrides)

    guice = new GuiceHelper
    guice.start(modules)

    // Ensure that service manager instance has been created
    guice.getInjector.getInstance(classOf[ServiceManager])
  }

  def shutdown(): Unit = guice.shutdown()
}
