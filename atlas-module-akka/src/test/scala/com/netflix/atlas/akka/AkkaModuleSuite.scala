/*
 * Copyright 2014-2019 Netflix, Inc.
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

import akka.actor.ActorSystem
import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.netflix.iep.guice.PreDestroyList
import com.netflix.iep.service.ServiceManager
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class AkkaModuleSuite extends AnyFunSuite {

  private val testCfg = ConfigFactory.parseString("""
      |atlas.akka.name = test
      |atlas.akka.port = 0
      |atlas.akka.actors = []
    """.stripMargin)

  test("load module") {
    val deps = new AbstractModule {

      override def configure(): Unit = {
        bind(classOf[Config]).toInstance(testCfg.withFallback(ConfigFactory.load()))
        bind(classOf[Registry]).toInstance(new NoopRegistry)
      }
    }
    // Module listed twice to verify dedup works
    val injector = Guice.createInjector(deps, new AkkaModule, new AkkaModule)
    assert(injector.getInstance(classOf[ActorSystem]) != null)
    assert(injector.getInstance(classOf[ServiceManager]).services().size === 2)
    injector.getInstance(classOf[PreDestroyList]).invokeAll()
  }
}
