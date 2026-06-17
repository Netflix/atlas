/*
 * Copyright 2014-2026 Netflix, Inc.
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

  /**
    * Intern using an explicit `timestamp` for recency rather than reading the clock. Lets code
    * processing a batch compute one timestamp and reuse it across many calls.
    */
  def intern(k: K, timestamp: Long): K

  /**
    * Probe for an already-interned value without inserting. The slot is located from `hashCode`
    * (which must equal the sought value's `hashCode`) and a candidate is confirmed with `isEqual`,
    * so a caller can probe with a reusable buffer and only allocate a permanent copy on a miss.
    * Returns the interned value or `null`. A hit refreshes recency with `timestamp` so the value
    * is not dropped by a subsequent [[retain]].
    */
  def get(hashCode: Int, isEqual: K => Boolean, timestamp: Long): K

  def retain(f: Long => Boolean): Unit

  def size: Int
}

class OpenHashInternMap[K <: AnyRef](initialCapacity: Int, clock: Clock = Clock.SYSTEM)
    extends InternMap[K] {

  private val primeCapacity = nextCapacity(initialCapacity)
  private var data = ArrayHelper.newInstance[K](primeCapacity)
  private var timestamps = new Array[Long](primeCapacity)
  private var currentSize = 0

  // The raw `hashCode` has weak high-bit dispersion, so mix it with `lowbias32`
  // before reducing into the slot range (the reduction keys off the high bits).
  private def slot(hashCode: Int): Int = {
    Hash.reduce(Hash.lowbias32(hashCode), data.length)
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
        internEntry(oldData(i), oldTimestamps(i))
      }
      i += 1
    }
  }

  private def internEntry(k: K, t: Long): K = {
    var j = slot(k.hashCode)
    var n = 0
    while (n < data.length) {
      if (data(j) == null) {
        currentSize += 1
        data(j) = k
        timestamps(j) = t
        return k
      } else if (data(j) == k) {
        timestamps(j) = t
        return data(j)
      }
      j = if (j + 1 < data.length) j + 1 else 0
      n += 1
    }
    throw new IllegalStateException("failed to add entry to map")
  }

  private def nextCapacity(sz: Int): Int = {
    PrimeFinder.nextPrime(sz)
  }

  private[util] def capacity: Int = data.length

  def intern(k: K): K = intern(k, clock.wallTime)

  def intern(k: K, timestamp: Long): K = {
    if (currentSize + 1 > 3 * data.length / 4) resize(nextCapacity(2 * data.length), _ => true)
    internEntry(k, timestamp)
  }

  def get(hashCode: Int, isEqual: K => Boolean, timestamp: Long): K = {
    var j = slot(hashCode)
    var n = 0
    while (n < data.length) {
      val d = data(j)
      if (d == null) {
        return null.asInstanceOf[K]
      } else if (isEqual(d)) {
        timestamps(j) = timestamp
        return d
      }
      j = if (j + 1 < data.length) j + 1 else 0
      n += 1
    }
    null.asInstanceOf[K]
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

  private def index(key: K): Int = index(key.hashCode)

  private def index(hashCode: Int): Int = {
    // Mix and reduce rather than `hashCode % concurrencyLevel`: the latter returns a
    // negative index for hashCode == Integer.MIN_VALUE (negation overflows), and keys off
    // the weak low bits. `reduce` is always in `[0, concurrencyLevel)`.
    Hash.reduce(Hash.lowbias32(hashCode), concurrencyLevel)
  }

  def intern(k: K): K = {
    val i = index(k)
    locks(i).lock()
    try segments(i).intern(k)
    finally {
      locks(i).unlock()
    }
  }

  def intern(k: K, timestamp: Long): K = {
    val i = index(k)
    locks(i).lock()
    try segments(i).intern(k, timestamp)
    finally {
      locks(i).unlock()
    }
  }

  def get(hashCode: Int, isEqual: K => Boolean, timestamp: Long): K = {
    val i = index(hashCode)
    locks(i).lock()
    try segments(i).get(hashCode, isEqual, timestamp)
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
