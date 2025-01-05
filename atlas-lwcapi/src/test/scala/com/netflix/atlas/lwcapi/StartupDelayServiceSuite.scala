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
package com.netflix.atlas.lwcapi

import java.time.Duration

import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class StartupDelayServiceSuite extends FunSuite {

  private def newInstance(delay: String): (StartupDelayService, ManualClock) = {
    val config = ConfigFactory.parseString(s"atlas.lwcapi.startup-delay = $delay")
    val clock = new ManualClock()
    new StartupDelayService(new DefaultRegistry(clock), config) -> clock
  }

  test("not healthy until started") {
    val (service, clock) = newInstance("3m")
    clock.setWallTime(Duration.ofMinutes(3).toMillis)
    assert(!service.isHealthy)
  }

  test("not healthy until after delay") {
    val (service, clock) = newInstance("3m")
    service.start()
    assert(!service.isHealthy)
    clock.setWallTime(Duration.ofMinutes(2).toMillis)
    assert(!service.isHealthy)
    clock.setWallTime(Duration.ofMinutes(3).toMillis)
    assert(service.isHealthy)
  }
}
