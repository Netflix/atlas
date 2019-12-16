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
package com.netflix.atlas.guice

import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.netflix.atlas.core.db.Database
import com.netflix.iep.guice.PreDestroyList
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class WebApiModuleSuite extends AnyFunSuite {

  test("load module") {
    val deps = new AbstractModule {

      override def configure(): Unit = {
        bind(classOf[Config]).toInstance(ConfigFactory.load())
      }
    }
    val injector = Guice.createInjector(deps, new WebApiModule, new WebApiModule)
    assert(injector.getInstance(classOf[Database]) != null)
    injector.getInstance(classOf[PreDestroyList]).invokeAll()
  }
}
