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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

import java.util.Optional

/**
  * Configures the actor system and web server. This module expects that bindings
  * are available for `com.typesafe.config.Config` and `com.netflix.spectator.api.Registry`.
  */
@Configuration
class PekkoConfiguration {

  @Bean
  def actorSystemService(config: Optional[Config]): ActorSystemService = {
    val c = config.orElseGet(() => loadConfig())
    new ActorSystemService(c)
  }

  @Bean
  def actorSystem(service: ActorSystemService): ActorSystem = {
    service.system
  }

  @Bean
  def materializerService(system: ActorSystem): MaterializerService = {
    new MaterializerService(system)
  }

  @Bean
  def materializer(service: MaterializerService): Materializer = {
    service.materializer
  }

  @Bean
  def actorService(
    system: ActorSystem,
    registry: Optional[Registry],
    factory: ClassFactory
  ): ActorService = {
    val r = registry.orElseGet(() => new NoopRegistry)
    new ActorService(system, system.settings.config, r, factory)
  }

  @Bean
  def webServer(
    factory: ClassFactory,
    registry: Optional[Registry],
    system: ActorSystem,
    materializer: Materializer
  ): WebServer = {
    val r = registry.orElseGet(() => new NoopRegistry)
    new WebServer(system.settings.config, factory, r, system, materializer)
  }

  private def loadConfig(): Config = {
    ConfigFactory.load()
  }
}
