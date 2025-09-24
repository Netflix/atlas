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

import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.actor.ActorSystem
import com.netflix.iep.service.ServiceManager
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import scala.util.Using

class PekkoConfigurationSuite extends FunSuite {

  private val testCfg = ConfigFactory.parseString("""
      |atlas.pekko.name = test
      |atlas.pekko.port = 0
      |atlas.pekko.actors = []
    """.stripMargin)

  test("load module") {
    Using.resource(new AnnotationConfigApplicationContext()) { context =>
      context.scan("com.netflix")
      context.registerBean(classOf[Config], () => testCfg.withFallback(ConfigFactory.load()))
      context.refresh()
      context.start()
      assert(context.getBean(classOf[ActorSystem]) != null)
      assert(context.getBean(classOf[ActorRefFactory]) != null)
      assertEquals(context.getBean(classOf[ServiceManager]).services().size(), 4)
    }
  }
}
