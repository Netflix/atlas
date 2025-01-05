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
package com.netflix.atlas.eval

import org.apache.pekko.actor.ActorSystem
import com.netflix.atlas.eval.stream.Evaluator
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

import java.util.Optional

/**
  * Configures a binding for an `Evaluator` instance.
  */
@Configuration
class EvalConfiguration {

  import EvalConfiguration.*

  @Bean
  def evaluator(
    config: Optional[Config],
    registry: Optional[Registry],
    system: Optional[ActorSystem]
  ): Evaluator = {
    val c = config.orElseGet(() => ConfigFactory.load(pickClassLoader))
    val r = registry.orElseGet(() => new NoopRegistry)
    val s = system.orElseGet(() => ActorSystem("EvalModule", c))
    new Evaluator(c, r, s)
  }
}

private object EvalConfiguration extends StrictLogging {

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
}
