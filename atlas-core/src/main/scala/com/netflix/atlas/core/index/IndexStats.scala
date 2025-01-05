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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.netflix.spectator.api.Id
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter

/**
  * Helper for reporting basic stats about the index. The three stats collected
  * are:
  *
  * 1. Number of key strings
  * 2. Number of value strings
  * 3. Number of items
  * 4. Number of metrics
  *
  * The number of strings help to determine how close we are to the size of the
  * string table used for interning. Items is mostly provided as a cross reference
  * to get a rough idea of the impact for rolling up a given key. Since the overlap
  * with other dimensions is not known, it will only be a rough estimate.
  *
  * @param registry
  *     Spectator registry to use for reporting the stats.
  */
class IndexStats(registry: Registry = new NoopRegistry) {

  import IndexStats.*

  private val numberOfValues = new ConcurrentHashMap[Id, ValueEntry]()
  private val numberOfMetrics = createGauge("atlas.db.size")
  private val numberOfKeys = createGauge("atlas.index.numberOfKeys")

  private def createGauge(name: String): AtomicLong = {
    PolledMeter
      .using(registry)
      .withName(name)
      .monitorValue(new AtomicLong(0L))
  }

  /**
    * Update stats that pertain to index as a whole, based on the latest build
    * of the index. This may widen to include any other useful information about
    * the index itself, such as size in memory, hash load factor, seek time etc.
    */
  def updateIndexStats(numItems: Int): Unit = {
    numberOfMetrics.set(numItems)
  }

  /**
    * Update key stats based on the latest build of the index.
    */
  def updateKeyStats(stats: List[KeyStat]): Unit = {
    numberOfKeys.set(stats.length)

    val now = registry.clock().wallTime()
    val n = 20
    val sorted = stats.sortWith(_.numValues > _.numValues)

    // Include the key for the top-N
    sorted.take(n).foreach { stat =>
      updateKeyStat("atlas.index.numberOfValues", stat.key, stat.numValues)
      updateKeyStat("atlas.index.numberOfItems", stat.key, stat.numItems)
    }

    // All others get aggregated
    var numValues = 0L
    var numItems = 0L
    sorted.drop(n).foreach { stat =>
      numValues += stat.numValues
      numItems += stat.numItems
    }
    updateKeyStat("atlas.index.numberOfValues", "-others-", numValues)
    updateKeyStat("atlas.index.numberOfItems", "-others-", numItems)

    // Remove older keys that have not been updated
    val iter = numberOfValues.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry.getValue.timestamp.get() < now) {
        PolledMeter.remove(registry, entry.getKey)
        registry.gauge(entry.getKey).set(Double.NaN)
        iter.remove()
      }
    }
  }

  private def updateKeyStat(name: String, key: String, count: Long): Unit = {
    val valueId = registry.createId(name, "key", key)
    val entry = numberOfValues.computeIfAbsent(
      valueId,
      id => {
        val gauge = PolledMeter
          .using(registry)
          .withId(id)
          .monitorValue(new AtomicLong())
        ValueEntry(new AtomicLong(registry.clock().wallTime()), gauge)
      }
    )
    entry.timestamp.set(registry.clock().wallTime())
    entry.count.set(count)
  }
}

object IndexStats {

  case class KeyStat(key: String, numItems: Long, numValues: Long)
  private case class ValueEntry(timestamp: AtomicLong, count: AtomicLong)
}
