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
package com.netflix.atlas.core.index

import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.patterns.PolledMeter
import munit.FunSuite

import scala.util.Random

class IndexStatsSuite extends FunSuite {

  private val clock = new ManualClock()
  private var registry = new DefaultRegistry(clock)
  private var stats = new IndexStats(registry)

  override def beforeEach(context: BeforeEach): Unit = {
    clock.setWallTime(0)
    registry = new DefaultRegistry(clock)
    stats = new IndexStats(registry)

    val keyStats = (0 until 50).map { i =>
      IndexStats.KeyStat(i.toString, 50 - i, i)
    }
    stats.updateKeyStats(keyStats.toList)
    stats.updateIndexStats(100)
    PolledMeter.update(registry)
  }

  test("expected number of gauges present") {
    // Should have 44 metrics in total:
    // 43 key metrics: 1 num keys, 21 num values, 21 num items
    //  1  db metrics: 1 num metrics
    assertEquals(registry.gauges().count(), 44L)
  }

  test("top-N value") {
    val numValues = registry.gauge("atlas.index.numberOfValues", "key", "42").value()
    assertEquals(numValues, 42.0)
  }

  test("top-N item") {
    val numItems = registry.gauge("atlas.index.numberOfItems", "key", "42").value()
    assertEquals(numItems, 8.0)
  }

  test("aggregation of other values") {
    // Top 20 are selected, others is N * (N - 1) / 2
    val numValues = registry.gauge("atlas.index.numberOfValues", "key", "-others-").value()
    assertEquals(numValues, 30.0 * 29 / 2)
  }

  test("aggregation of other items") {
    // Top 20 by value are selected, items were given reverse counts
    val numItems = registry.gauge("atlas.index.numberOfItems", "key", "-others-").value()
    val overall = 50.0 * 51 / 2
    val top20 = 20 * 21 / 2
    assertEquals(numItems, overall - top20)
  }

  test("cleanup") {
    clock.setWallTime(60)
    val keyStats = (0 until 50).map { i =>
      IndexStats.KeyStat(i.toString, 50 - i, if (i == 42) 2 else i)
    }
    stats.updateKeyStats(keyStats.toList)
    PolledMeter.update(registry)
    val numValues = registry.gauge("atlas.index.numberOfValues", "key", "42").value()
    assert(numValues.isNaN)
  }

  test("sort with duplicates") {
    (0 until 100).foreach { i =>
      clock.setWallTime(i)
      val keyStats = (0 until 100).map { j =>
        IndexStats.KeyStat(j.toString, i, Random.nextInt(10))
      }
      stats.updateKeyStats(keyStats.toList)
    }
  }

  test("db size is present") {
    val numMetrics = registry.gauge("atlas.db.size").value()
    assertEquals(numMetrics, 100.0)
  }
}
