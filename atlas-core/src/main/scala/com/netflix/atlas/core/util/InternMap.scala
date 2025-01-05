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
package com.netflix.atlas.core.util

import java.util.concurrent.locks.ReentrantLock

import com.netflix.spectator.api.Clock

object InternMap {

  def concurrent[K <: AnyRef](
    initialCapacity: Int,
    clock: Clock = Clock.SYSTEM,
    concurrencyLevel: Int = 16
  ): InternMap[K] = {
    val segmentCapacity = initialCapacity / concurrencyLevel + concurrencyLevel
    new ConcurrentInternMap[K](new OpenHashInternMap[K](segmentCapacity, clock), concurrencyLevel)
  }
}

trait InternMap[K <: AnyRef] extends Interner[K] {

  def intern(k: K): K

  def retain(f: Long => Boolean): Unit

  def size: Int
}

class OpenHashInternMap[K <: AnyRef](initialCapacity: Int, clock: Clock = Clock.SYSTEM)
    extends InternMap[K] {

  private val primeCapacity = nextCapacity(initialCapacity)
  private var data = ArrayHelper.newInstance[K](primeCapacity)
  private var timestamps = new Array[Long](primeCapacity)
  private var currentSize = 0

  private def hash(k: K): Int = {
    val h = k.hashCode % data.length
    if (h < 0) -h else h
  }

  private def resize(newCapacity: Int, keep: Long => Boolean): Unit = {
    val oldData = data
    val oldTimestamps = timestamps

    currentSize = 0
    data = ArrayHelper.newInstance[K](newCapacity)
    timestamps = new Array[Long](newCapacity)
    var i = 0
    while (i < oldData.length) {
      if (oldData(i) != null && keep(oldTimestamps(i))) {
        intern(oldData(i), oldTimestamps(i))
      }
      i += 1
    }
  }

  private def intern(k: K, t: Long): K = {
    val h = hash(k)
    var i = h
    while (i < h + data.length) {
      val j = i % data.length
      if (data(j) == null) {
        currentSize += 1
        data(j) = k
        timestamps(j) = t
        return k
      } else if (data(j) == k) {
        timestamps(j) = t
        return data(j)
      }
      i += 1
    }
    throw new IllegalStateException("failed to add entry to map")
  }

  private def nextCapacity(sz: Int): Int = {
    PrimeFinder.nextPrime(sz)
  }

  private[util] def capacity: Int = data.length

  def intern(k: K): K = {
    if (currentSize + 1 > 3 * data.length / 4) resize(nextCapacity(2 * data.length), _ => true)
    intern(k, clock.wallTime)
  }

  def retain(f: Long => Boolean): Unit = {
    resize(data.length, f)
  }

  def size: Int = currentSize
}

class ConcurrentInternMap[K <: AnyRef](newMap: => InternMap[K], concurrencyLevel: Int)
    extends InternMap[K] {

  private val segments = new Array[InternMap[K]](concurrencyLevel)
  private val locks = new Array[ReentrantLock](concurrencyLevel)

  // Initialize segment maps and locks
  (0 until concurrencyLevel).foreach { i =>
    segments(i) = newMap
    locks(i) = new ReentrantLock
  }

  private def index(key: K): Int = {
    val k = key.hashCode
    val i = k % concurrencyLevel
    if (i < 0) -1 * i else i
  }

  def intern(k: K): K = {
    val i = index(k)
    locks(i).lock()
    try segments(i).intern(k)
    finally {
      locks(i).unlock()
    }
  }

  def retain(f: Long => Boolean): Unit = {
    (0 until concurrencyLevel).foreach { i =>
      locks(i).lock()
      try segments(i).retain(f)
      finally {
        locks(i).unlock()
      }
    }
  }

  def size: Int = {
    var totalSize = 0
    (0 until concurrencyLevel).foreach { i =>
      locks(i).lock()
      try totalSize += segments(i).size
      finally {
        locks(i).unlock()
      }
    }
    totalSize
  }
}
