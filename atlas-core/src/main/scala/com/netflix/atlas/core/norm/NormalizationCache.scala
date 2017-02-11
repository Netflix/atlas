/*
 * Copyright 2014-2017 Netflix, Inc.
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

import java.util.LinkedHashMap

import com.netflix.atlas.core.model.BasicTaggedItem
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.spectator.api.Clock


class NormalizationCache(step: Long, updateF: Datapoint => Unit, clock: Clock = Clock.SYSTEM) {

  /**
   * For small step sizes use a fixed two minute heartbeat as reporting can be irregular, for
   * larger step size allow one sample to be missed.
   */
  private val heartbeat = if (step < 60000) 120000 else 2 * step

  type CacheEntry = java.util.Map.Entry[TaggedItem, CacheValue]

  private val counterCache = new LinkedHashMap[TaggedItem, CacheValue](16, 0.75f, true) {
    override def removeEldestEntry(eldest: CacheEntry): Boolean = {
      val ageMillis = clock.wallTime - eldest.getValue.lastAccessTime
      ageMillis > 4 * step
    }
  }

  private val rateCache = new LinkedHashMap[TaggedItem, CacheValue](16, 0.75f, true) {
    override def removeEldestEntry(eldest: CacheEntry): Boolean = {
      val ageMillis = clock.wallTime - eldest.getValue.lastAccessTime
      ageMillis > 4 * step
    }
  }

  private val gaugeCache = new LinkedHashMap[TaggedItem, CacheValue](16, 0.75f, true) {
    override def removeEldestEntry(eldest: CacheEntry): Boolean = {
      val ageMillis = clock.wallTime - eldest.getValue.lastAccessTime
      ageMillis > 4 * step
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
      val norm = new NormalizeValueFunction(step, heartbeat, update)
      value = new CacheValue(clock.wallTime, norm)
      rateCache.put(meta, value)
    }
    value.lastAccessTime = clock.wallTime
    value.f(m.timestamp, m.value)
  }

  def updateGauge(m: Datapoint): Unit = {
    val meta = BasicTaggedItem(m.tags)
    var value = gaugeCache.get(meta)
    if (value == null) {
      val update = new UpdateValueFunction(meta, step, updateF)
      val norm = new LastValueFunction(step, update)
      value = new CacheValue(clock.wallTime, norm)
      gaugeCache.put(meta, value)
    }
    value.lastAccessTime = clock.wallTime
    value.f(m.timestamp, m.value)
  }

  class CacheValue(var lastAccessTime: Long, val f: ValueFunction)
}
