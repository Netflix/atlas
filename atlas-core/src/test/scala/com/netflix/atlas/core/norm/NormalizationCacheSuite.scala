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
package com.netflix.atlas.core.norm

import com.netflix.atlas.core.model.Datapoint
import com.netflix.spectator.api.ManualClock
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class NormalizationCacheSuite extends AnyFunSuite with BeforeAndAfter {

  val clock = new ManualClock()
  val buffer = scala.collection.mutable.Buffer.empty[Datapoint]
  val cache = new NormalizationCache(10, buffer += _, clock)

  def dp(t: Long, v: Double): Datapoint = Datapoint(Map.empty, t, v)

  before {
    buffer.clear()
  }

  test("update rate on exact interval") {
    clock.setWallTime(11)
    cache.updateRate(dp(10, 1.0))
    clock.setWallTime(21)
    cache.updateRate(dp(20, 1.0))

    // skip interval, https://github.com/Netflix/atlas/issues/497

    clock.setWallTime(41)
    cache.updateRate(dp(40, 1.0))

    val expected = List(
      dp(10, 1.0),
      dp(20, 1.0),
      dp(40, 1.0)
    )
    val actual = buffer.toList

    assert(actual === expected)
  }

  test("update counter on exact interval") {
    clock.setWallTime(11)
    cache.updateCounter(dp(10, 0.0))
    clock.setWallTime(21)
    cache.updateCounter(dp(20, 1.0))

    // skip interval

    clock.setWallTime(41)
    cache.updateCounter(dp(40, 2.0))

    val expected = List(
      dp(20, 100.0),
      dp(30, 50.0),
      dp(40, 50.0)
    )
    val actual = buffer.toList

    assert(actual === expected)
  }

  test("update gauge on exact interval") {
    clock.setWallTime(11)
    cache.updateGauge(dp(10, 0.0))
    clock.setWallTime(21)
    cache.updateGauge(dp(20, 1.0))

    // skip interval

    clock.setWallTime(41)
    cache.updateGauge(dp(40, 2.0))

    val expected = List(
      dp(10, 0.0),
      dp(20, 1.0),
      dp(40, 2.0)
    )
    val actual = buffer.toList

    assert(actual === expected)
  }
}
