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
package com.netflix.atlas.guice

import java.util.concurrent.TimeUnit

import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.netflix.atlas.akka.ActorService
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class CloudWatchModuleSuite extends FunSuite {

  test("load module") {
    val deps = new AbstractModule {
      override def configure(): Unit = {
        bind(classOf[Config]).toInstance(ConfigFactory.load())
        bind(classOf[Registry]).toInstance(new DefaultRegistry())
      }
    }
    val injector = Guice.createInjector(deps, new CloudWatchModule, new CloudWatchModule)
    val actors = injector.getInstance(classOf[ActorService])
    assert(actors.isHealthy)
    actors.stop()
  }

  ignore("run locally") {
    val deps = new AbstractModule {
      override def configure(): Unit = {
        bind(classOf[Config]).toInstance(ConfigFactory.load("local"))
        bind(classOf[Registry]).toInstance(new DefaultRegistry())
      }
    }
    val injector = Guice.createInjector(deps, new CloudWatchModule)
    val actors = injector.getInstance(classOf[ActorService])
    assert(actors.isHealthy)
    Thread.sleep(TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES))
    actors.stop()
  }
}
