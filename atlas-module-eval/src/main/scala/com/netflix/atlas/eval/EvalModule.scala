/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.eval

import javax.inject.Singleton

import akka.actor.ActorSystem
import com.google.inject.AbstractModule
import com.google.inject.Inject
import com.google.inject.Provides
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

/**
  * Configures a binding for an [[Evaluator]] instance.
  */
final class EvalModule extends AbstractModule {

  import EvalModule._

  override def configure(): Unit = {}

  @Provides
  @Singleton
  protected def providesEvaluator(opts: OptionalInjections): Evaluator = {
    new Evaluator(opts.config, opts.registry, opts.getActorSystem)
  }

  override def equals(obj: Any): Boolean = {
    obj != null && getClass.equals(obj.getClass)
  }

  override def hashCode(): Int = getClass.hashCode()
}

private object EvalModule extends StrictLogging {

  private def pickClassLoader: ClassLoader = {
    val cl = Thread.currentThread().getContextClassLoader
    if (cl != null) cl
    else {
      val cname = getClass.getName
      logger.warn(
        s"Thread.currentThread().getContextClassLoader() is null, using loader for $cname"
      )
      getClass.getClassLoader
    }
  }

  private class OptionalInjections {

    @Inject(optional = true)
    var config: Config = ConfigFactory.load(pickClassLoader)

    @Inject(optional = true)
    var registry: Registry = new NoopRegistry

    @Inject(optional = true)
    var system: ActorSystem = _

    def getActorSystem: ActorSystem = {
      if (system == null) {
        system = ActorSystem("EvalModule", config)
      }
      system
    }
  }
}
