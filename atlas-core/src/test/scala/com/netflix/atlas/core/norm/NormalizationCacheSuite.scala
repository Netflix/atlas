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
package com.netflix.atlas.core.norm

import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.spectator.api.ManualClock
import munit.FunSuite

class NormalizationCacheSuite extends FunSuite {

  private val clock = new ManualClock()
  private val buffer = scala.collection.mutable.Buffer.empty[DatapointTuple]

  private val updateF: UpdateFunction = { (id, tags, ts, v) =>
    buffer += DatapointTuple(id, tags, ts, v)
  }
  private val cache = new NormalizationCache(10, updateF, clock)

  private val tags = Map.empty[String, String]
  private val id = TaggedItem.computeId(tags)

  def dp(t: Long, v: Double): DatapointTuple = DatapointTuple(id, tags, t, v)

  override def beforeEach(context: BeforeEach): Unit = {
    buffer.clear()
  }

  test("update rate on exact interval") {
    clock.setWallTime(11)
    cache.updateRate(id, tags, 10, 1.0)
    clock.setWallTime(21)
    cache.updateRate(id, tags, 20, 1.0)

    // skip interval, https://github.com/Netflix/atlas/issues/497

    clock.setWallTime(41)
    cache.updateRate(id, tags, 40, 1.0)

    val expected = List(
      dp(10, 1.0),
      dp(20, 1.0),
      dp(40, 1.0)
    )
    val actual = buffer.toList

    assertEquals(actual, expected)
  }

  test("update counter on exact interval") {
    clock.setWallTime(11)
    cache.updateCounter(id, tags, 10, 0.0)
    clock.setWallTime(21)
    cache.updateCounter(id, tags, 20, 1.0)

    // skip interval

    clock.setWallTime(41)
    cache.updateCounter(id, tags, 40, 2.0)

    val expected = List(
      dp(20, 100.0),
      dp(30, 50.0),
      dp(40, 50.0)
    )
    val actual = buffer.toList

    assertEquals(actual, expected)
  }

  test("update gauge on exact interval") {
    clock.setWallTime(11)
    cache.updateGauge(id, tags, 10, 0.0)
    clock.setWallTime(21)
    cache.updateGauge(id, tags, 20, 1.0)

    // skip interval

    clock.setWallTime(41)
    cache.updateGauge(id, tags, 40, 2.0)

    val expected = List(
      dp(10, 0.0),
      dp(20, 1.0),
      dp(40, 2.0)
    )
    val actual = buffer.toList

    assertEquals(actual, expected)
  }
}
