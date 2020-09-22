/*
 * Copyright 2014-2020 Netflix, Inc.
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
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.multibindings.Multibinder
import com.netflix.iep.guice.LifecycleModule
import com.netflix.iep.service.Service
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import javax.inject.Provider

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Configures the actor system and web server. This module expects that bindings
  * are available for [[com.typesafe.config.Config]] and [[com.netflix.spectator.api.Registry]].
  */
final class AkkaModule extends AbstractModule {

  override def configure(): Unit = {
    install(new LifecycleModule)
    bind(classOf[ActorSystem]).toProvider(classOf[AkkaModule.ActorSystemProvider])
    bind(classOf[Materializer]).toProvider(classOf[AkkaModule.MaterializerProvider])

    // Mark as eager to ensure they will be created
    bind(classOf[ActorService]).asEagerSingleton()
    bind(classOf[WebServer]).asEagerSingleton()

    // Hookup to service manager for health tracking
    val serviceBinder = Multibinder.newSetBinder(binder, classOf[Service])
    serviceBinder.addBinding().to(classOf[ActorService])
    serviceBinder.addBinding().to(classOf[WebServer])
  }

  @Provides
  @Singleton
  protected def providesActorRefFactory(system: ActorSystem): ActorRefFactory = system

  override def equals(obj: Any): Boolean = {
    obj != null && getClass.equals(obj.getClass)
  }

  override def hashCode(): Int = getClass.hashCode()
}

object AkkaModule extends StrictLogging {

  @Singleton
  private class ActorSystemProvider @Inject() (config: Config)
      extends Provider[ActorSystem]
      with AutoCloseable {

    private val system = ActorSystem(config.getString("atlas.akka.name"), config)

    override def get(): ActorSystem = system

    override def close(): Unit = {
      Await.ready(system.terminate(), Duration.Inf)
    }
  }

  @Singleton
  private class MaterializerProvider @Inject() (system: ActorSystem, registry: Registry)
      extends Provider[Materializer]
      with AutoCloseable {

    private val materializer = StreamOps.materializer(system, registry)

    override def get(): Materializer = materializer

    override def close(): Unit = {
      materializer.shutdown()
    }
  }
}
