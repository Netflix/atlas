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

import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.util.Math
import com.netflix.spectator.api.Clock

class NormalizationCache(step: Long, updateF: UpdateFunction, clock: Clock = Clock.SYSTEM) {

  import NormalizationCache.*

  /**
    * For small step sizes use a fixed two minute heartbeat as reporting can be irregular, for
    * larger step size allow one sample to be missed.
    */
  private val heartbeat = if (step < 60000) 120000 else 2 * step

  type CacheEntry = java.util.Map.Entry[ItemId, CacheValue]

  private val counterCache = newCacheMap()

  private val rateCache = newCacheMap()

  private val sumCache = newCacheMap()

  private val gaugeCache = newCacheMap()

  private def newCacheMap(): java.util.LinkedHashMap[ItemId, CacheValue] = {
    new java.util.LinkedHashMap[ItemId, CacheValue](16, 0.75f, true) {

      override def removeEldestEntry(eldest: CacheEntry): Boolean = {
        val ageMillis = clock.wallTime - eldest.getValue.lastAccessTime
        if (ageMillis > 4 * step) {
          eldest.getValue.f.close()
          true
        } else {
          false
        }
      }
    }
  }

  def updateCounter(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit = {
    var cacheValue = counterCache.get(id)
    if (cacheValue == null) {
      val update = new UpdateValueFunction(id, tags, updateF)
      val norm = new NormalizeValueFunction(step, heartbeat, update)
      val rate = new RateValueFunction(norm)
      cacheValue = new CacheValue(clock.wallTime, rate)
      counterCache.put(id, cacheValue)
    }
    cacheValue.lastAccessTime = clock.wallTime
    cacheValue.f(timestamp, value)
  }

  def updateRate(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit = {
    var cacheValue = rateCache.get(id)
    if (cacheValue == null) {
      val update = new UpdateValueFunction(id, tags, updateF)
      // If the client is already converting to a rate, then do not use a heartbeat that is
      // larger than the step size as it can cause over counting for the final result. For
      // more details see:
      // https://github.com/Netflix/atlas/issues/497
      val norm = new NormalizeValueFunction(step, step, update)
      cacheValue = new CacheValue(clock.wallTime, norm)
      rateCache.put(id, cacheValue)
    }
    cacheValue.lastAccessTime = clock.wallTime
    cacheValue.f(timestamp, value)
  }

  def updateSum(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit = {
    var cacheValue = sumCache.get(id)
    if (cacheValue == null) {
      val update = new UpdateValueFunction(id, tags, updateF)
      val norm = new RollingValueFunction(step, Math.addNaN, update)
      cacheValue = new CacheValue(clock.wallTime, norm)
      sumCache.put(id, cacheValue)
    }
    cacheValue.lastAccessTime = clock.wallTime
    cacheValue.f(timestamp, value)
  }

  def updateGauge(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit = {
    var cacheValue = gaugeCache.get(id)
    if (cacheValue == null) {
      val update = new UpdateValueFunction(id, tags, updateF)
      val norm = new RollingValueFunction(step, Math.maxNaN, update)
      cacheValue = new CacheValue(clock.wallTime, norm)
      gaugeCache.put(id, cacheValue)
    }
    cacheValue.lastAccessTime = clock.wallTime
    cacheValue.f(timestamp, value)
  }

}

object NormalizationCache {

  class CacheValue(var lastAccessTime: Long, val f: ValueFunction)
}
