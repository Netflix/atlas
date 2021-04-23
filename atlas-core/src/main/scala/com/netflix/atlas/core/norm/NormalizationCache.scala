/*
 * Copyright 2014-2021 Netflix, Inc.
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

import com.netflix.atlas.core.model.BasicTaggedItem
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.Math
import com.netflix.spectator.api.Clock

class NormalizationCache(step: Long, updateF: Datapoint => Unit, clock: Clock = Clock.SYSTEM) {

  /**
    * For small step sizes use a fixed two minute heartbeat as reporting can be irregular, for
    * larger step size allow one sample to be missed.
    */
  private val heartbeat = if (step < 60000) 120000 else 2 * step

  type CacheEntry = java.util.Map.Entry[TaggedItem, CacheValue]

  private val counterCache = newCacheMap()

  private val rateCache = newCacheMap()

  private val sumCache = newCacheMap()

  private val gaugeCache = newCacheMap()

  private def newCacheMap(): java.util.LinkedHashMap[TaggedItem, CacheValue] = {
    new java.util.LinkedHashMap[TaggedItem, CacheValue](16, 0.75f, true) {

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

  def updateCounter(m: Datapoint): Unit = {
    val meta = BasicTaggedItem(m.tags)
    var value = counterCache.get(meta)
    if (value == null) {
      val update = new UpdateValueFunction(meta, step, updateF)
      val norm = new NormalizeValueFunction(step, heartbeat, update)
      val rate = new RateValueFunction(norm)
      value = new CacheValue(clock.wallTime, rate)
      counterCache.put(meta, value)
    }
    value.lastAccessTime = clock.wallTime
    value.f(m.timestamp, m.value)
  }

  def updateRate(m: Datapoint): Unit = {
    val meta = BasicTaggedItem(m.tags)
    var value = rateCache.get(meta)
    if (value == null) {
      val update = new UpdateValueFunction(meta, step, updateF)
      // If the client is already converting to a rate, then do not use a heartbeat that is
      // larger than the step size as it can cause over counting for the final result. For
      // more details see:
      // https://github.com/Netflix/atlas/issues/497
      val norm = new NormalizeValueFunction(step, step, update)
      value = new CacheValue(clock.wallTime, norm)
      rateCache.put(meta, value)
    }
    value.lastAccessTime = clock.wallTime
    value.f(m.timestamp, m.value)
  }

  def updateSum(m: Datapoint): Unit = {
    val meta = BasicTaggedItem(m.tags)
    var value = sumCache.get(meta)
    if (value == null) {
      val update = new UpdateValueFunction(meta, step, updateF)
      val norm = new RollingValueFunction(step, Math.addNaN, update)
      value = new CacheValue(clock.wallTime, norm)
      sumCache.put(meta, value)
    }
    value.lastAccessTime = clock.wallTime
    value.f(m.timestamp, m.value)
  }

  def updateGauge(m: Datapoint): Unit = {
    val meta = BasicTaggedItem(m.tags)
    var value = gaugeCache.get(meta)
    if (value == null) {
      val update = new UpdateValueFunction(meta, step, updateF)
      val norm = new RollingValueFunction(step, Math.maxNaN, update)
      value = new CacheValue(clock.wallTime, norm)
      gaugeCache.put(meta, value)
    }
    value.lastAccessTime = clock.wallTime
    value.f(m.timestamp, m.value)
  }

  class CacheValue(var lastAccessTime: Long, val f: ValueFunction)
}
