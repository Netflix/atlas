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
    val n = data.length
    // Anchor the scan just after an empty slot. A backward-shift removal never moves an entry
    // past an empty slot, so anchoring there guarantees a removal cannot push an un-scanned
    // (possibly expired) entry behind the scan cursor into an already-visited slot.
    var anchor = 0
    while (anchor < n && data(anchor) != null) anchor += 1
    if (anchor == n) {
      // Table is completely full (cannot happen below the 3/4 load factor); fall back to a rehash.
      resize(n, f)
    } else {
      var count = 0
      var i = if (anchor + 1 < n) anchor + 1 else 0
      while (count < n) {
        // A removal may shift another entry (also possibly expired) into slot `i`, so drain
        // until the slot is empty or holds a kept entry.
        while (data(i) != null && !f(timestamps(i))) {
          removeAt(i)
        }
        i = if (i + 1 < n) i + 1 else 0
        count += 1
      }
    }
  }

  // Backward-shift deletion for linear probing (Knuth Algorithm R): remove the entry at `idx`
  // and slide back any following entry that can legally fill the gap, so no probe chain is
  // broken and no tombstone is left behind.
  private def removeAt(idx: Int): Unit = {
    val n = data.length
    var gap = idx
    var j = if (idx + 1 < n) idx + 1 else 0
    while (data(j) != null) {
      val h = slot(data(j).hashCode)
      // Move the entry at `j` back into the gap unless its home slot `h` lies in `(gap, j]`
      // (cyclically); moving it then would place it before its home and make it unreachable.
      val homeInRange =
        if (gap < j) h > gap && h <= j
        else h > gap || h <= j
      if (!homeInRange) {
        data(gap) = data(j)
        timestamps(gap) = timestamps(j)
        gap = j
      }
      j = if (j + 1 < n) j + 1 else 0
    }
    data(gap) = null.asInstanceOf[K]
    currentSize -= 1
  }

  // Reference implementation of retain via full rehash, kept for differential testing and
  // benchmarking against the in-place [[retain]].
  private[util] def retainByRehash(f: Long => Boolean): Unit = {
    resize(data.length, f)
  }

  /** Probe for `k`; true if present and reachable. Test/benchmark helper. */
  private[util] def contains(k: K): Boolean = {
    val n = data.length
    var j = slot(k.hashCode)
    var count = 0
    while (count < n) {
      val d = data(j)
      if (d == null) return false
      else if (d == k) return true
      j = if (j + 1 < n) j + 1 else 0
      count += 1
    }
    false
  }

  /** Snapshot of the current entries (array contents). Test helper. */
  private[util] def snapshot: Set[K] = {
    val builder = Set.newBuilder[K]
    var i = 0
    while (i < data.length) {
      if (data(i) != null) builder += data(i)
      i += 1
    }
    builder.result()
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
