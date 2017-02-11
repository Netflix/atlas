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
package com.netflix.atlas.akka

import javax.inject.Singleton

import akka.actor.ActorSystem
import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.multibindings.Multibinder
import com.netflix.iep.guice.LifecycleModule
import com.netflix.iep.service.Service
import com.typesafe.config.Config

/**
  * Configures the actor system and web server. This module expects that bindings
  * are available for [[com.typesafe.config.Config]] and [[com.netflix.spectator.api.Registry]].
  */
final class AkkaModule extends AbstractModule {
  override def configure(): Unit = {
    install(new LifecycleModule)
    val serviceBinder = Multibinder.newSetBinder(binder, classOf[Service])
    serviceBinder.addBinding().to(classOf[ActorService])
    serviceBinder.addBinding().to(classOf[WebServer])
  }

  @Provides @Singleton
  private def providesActorSystem(config: Config): ActorSystem = {
    val name = config.getString("atlas.akka.name")
    ActorSystem(name, config)
  }

  override def equals(obj: Any): Boolean = {
    obj != null && getClass.equals(obj.getClass)
  }

  override def hashCode(): Int = getClass.hashCode()
}

