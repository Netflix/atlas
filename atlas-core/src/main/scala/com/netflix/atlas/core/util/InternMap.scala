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

  /**
    * Create a concurrent interner backed by independently locked segments.
    *
    * @param initialCapacity
    *     Total expected number of entries. It is divided across the segments.
    * @param clock
    *     Clock used to timestamp entries for recency-based [[InternMap.retain]].
    * @param concurrencyLevel
    *     Number of striped-lock segments. A larger value lowers the chance that concurrent
    *     threads contend on the same lock, so it should comfortably exceed the number of threads
    *     that intern at once. The default is chosen to keep contention low for common thread-pool
    *     sizes (up to roughly 64 threads); use a larger value if more threads may intern
    *     concurrently. Additional segments cost only a few locks, not data memory, since the
    *     capacity is split among them.
    */
  def concurrent[K <: AnyRef](
    initialCapacity: Int,
    clock: Clock = Clock.SYSTEM,
    concurrencyLevel: Int = 128
  ): InternMap[K] = {
    val segmentCapacity = initialCapacity / concurrencyLevel + concurrencyLevel
    new ConcurrentInternMap[K](new OpenHashInternMap[K](segmentCapacity, clock), concurrencyLevel)
  }

  /**
    * Probe used by [[InternMap.getOrIntern]] to look up a value with a reusable buffer and build
    * it only on a miss. `matches` confirms a candidate by content; `create` builds the value to
    * insert when the slot is empty. Implemented as an abstract class with a primitive `Boolean`
    * result rather than a `Function1[K, Boolean]` so the per-probe comparison does not box the
    * result or allocate a closure for the factory.
    */
  abstract class InternProbe[K <: AnyRef] {

    /** True if `value` is the one being sought (compared by content against the probe buffer). */
    def matches(value: K): Boolean

    /** Build the value to insert. Called only on a miss, so a hit allocates nothing. */
    def create(): K
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

  /**
    * Look up the value identified by `hashCode`/`probe` and return it, interning a newly built
    * copy (`probe.create()`) if it is not already present. Combines the probe and insert into a
    * single chain walk, so a miss does not scan twice, and a hit allocates nothing.
    *
    * `hashCode` must equal the `hashCode` of the value that `probe` matches and `create()` builds.
    * The entry is stored at the slot derived from `hashCode`, so a mismatch would place it where
    * neither a later `get`/`getOrIntern` (by the real hash) nor `intern` would look, silently
    * defeating deduplication.
    */
  def getOrIntern(hashCode: Int, probe: InternMap.InternProbe[K], timestamp: Long): K

  def retain(f: Long => Boolean): Unit

  def size: Int

  /** (average, max) linear-probe distance from the home slot. Exposed for the distribution test. */
  private[util] def probeStats: (Double, Int)
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
      val d = data(j)
      if (d == null) {
        currentSize += 1
        data(j) = k
        timestamps(j) = t
        return k
      } else if ((d eq k) || d.equals(k)) {
        // `K <: AnyRef`, so `d.equals(k)` is a direct virtual call. Using `==` here would route
        // through `BoxesRunTime.equals`, adding a Number/Character instanceof dispatch on every
        // comparison. The `eq` fast-path skips `equals` when the same instance is re-interned.
        timestamps(j) = t
        return d
      }
      j = if (j + 1 < data.length) j + 1 else 0
      n += 1
    }
    throw new IllegalStateException("failed to add entry to map")
  }

  def getOrIntern(hashCode: Int, probe: InternMap.InternProbe[K], timestamp: Long): K = {
    if (currentSize + 1 > 3 * data.length / 4) resize(nextCapacity(2 * data.length), _ => true)
    var j = slot(hashCode)
    var n = 0
    while (n < data.length) {
      val d = data(j)
      if (d == null) {
        val k = probe.create()
        currentSize += 1
        data(j) = k
        timestamps(j) = timestamp
        return k
      } else if (probe.matches(d)) {
        timestamps(j) = timestamp
        return d
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
      else if ((d eq k) || d.equals(k)) return true
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

  private[util] def probeStats: (Double, Int) = {
    var total = 0L
    var max = 0
    var cnt = 0
    val len = data.length
    var i = 0
    while (i < len) {
      val d = data(i)
      if (d != null) {
        val home = slot(d.hashCode)
        val dist = if (i >= home) i - home else len - home + i
        total += dist
        if (dist > max) max = dist
        cnt += 1
      }
      i += 1
    }
    (if (cnt == 0) 0.0 else total.toDouble / cnt, max)
  }
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
    // Decorrelate the segment selection from the in-segment slot. The segment's `slot` reduces
    // `lowbias32(hashCode)`, and `Hash.reduce` keys off the HIGH bits of its input. Reducing the
    // same mixed value at both levels makes them key off overlapping high bits, so every entry in
    // a segment lands in a tiny slot range (capacity / concurrencyLevel^2 slots) — catastrophic
    // clustering that worsens as concurrencyLevel grows. Bit-reversing first makes this level key
    // off the LOW bits of the mix, which the slot ignores, so the two levels are independent.
    Hash.reduce(Integer.reverse(Hash.lowbias32(hashCode)), concurrencyLevel)
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

  def getOrIntern(hashCode: Int, probe: InternMap.InternProbe[K], timestamp: Long): K = {
    val i = index(hashCode)
    locks(i).lock()
    try segments(i).getOrIntern(hashCode, probe, timestamp)
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

  private[util] def probeStats: (Double, Int) = {
    var weighted = 0.0
    var cnt = 0
    var max = 0
    (0 until concurrencyLevel).foreach { i =>
      locks(i).lock()
      try {
        val (avg, m) = segments(i).probeStats
        val sz = segments(i).size
        weighted += avg * sz
        cnt += sz
        if (m > max) max = m
      } finally {
        locks(i).unlock()
      }
    }
    (if (cnt == 0) 0.0 else weighted / cnt, max)
  }
}
