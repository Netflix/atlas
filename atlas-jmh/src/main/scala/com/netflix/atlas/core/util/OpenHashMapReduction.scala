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

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

import scala.util.Random

/**
  * Measures the open-addressed maps in atlas-core on both the build (insert) and query
  * (get) paths. It exercises whatever slot-index reduction scheme is live in the current
  * checkout; to compare schemes (e.g. prime+modulo vs Lemire multiply-shift), run the same
  * benchmark on each branch and diff the results rather than switching schemes within this
  * file. Both paths matter because the rebuild profile is dominated by `get`/`intern`
  * lookups where the per-probe reduction is paid on every collision hop.
  *
  * The maps themselves are the unit under test; this benchmark is meant to be
  * run on `main` (baseline) and on each prototype branch with identical
  * arguments, then diffed:
  *
  * ```
  * > jmh:run -wi 5 -i 10 -f1 -t1 .*OpenHashMapReduction.*
  * ```
  *
  * Key sets are chosen to mirror the rebuild path:
  *   - `String` tag-like values for RefIntHashMap / OpenHashInternMap
  *   - dense-ish ints for IntRefHashMap / IntIntHashMap / IntHashSet
  *   - longs (item-id style) for LongHashSet / LongIntHashMap
  *
  * `N` is sized so the maps resize several times (exercising the re-insertion
  * probe loops) and settle well above the trivial-capacity range.
  */
@State(Scope.Thread)
class OpenHashMapReduction {

  import OpenHashMapReduction.*

  // Distinct keys to insert. Kept fixed across the build/query split so the
  // query phase looks up keys that are present (hit) interleaved with absent
  // keys (miss), which is where probe-walk length matters most.
  private var intKeys: Array[Int] = _
  private var longKeys: Array[Long] = _
  private var strKeys: Array[String] = _
  private var strMiss: Array[String] = _
  private var intMiss: Array[Int] = _

  // Pre-built maps for the query benchmarks.
  private var refIntMap: RefIntHashMap[String] = _
  private var intRefMap: IntRefHashMap[String] = _
  private var intIntMap: IntIntHashMap = _
  private var longIntMap: LongIntHashMap = _
  private var interner: InternMap[String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rnd = new Random(42)

    intKeys = Array.fill(N)(rnd.nextInt())
    longKeys = Array.fill(N)(rnd.nextLong())
    strKeys = Array.tabulate(N)(i => s"i-$i.us-east-1.${rnd.nextInt(1 << 20)}")
    // Absent keys for miss-path lookups.
    intMiss = Array.fill(N)(rnd.nextInt())
    strMiss = Array.tabulate(N)(i => s"absent-$i-${rnd.nextInt(1 << 20)}")

    refIntMap = new RefIntHashMap[String]
    intRefMap = new IntRefHashMap[String](Int.MinValue)
    intIntMap = new IntIntHashMap(-1)
    longIntMap = new LongIntHashMap(-1L)
    val sentinel = "v"
    var i = 0
    while (i < N) {
      refIntMap.increment(strKeys(i))
      intRefMap.put(intKeys(i), sentinel)
      intIntMap.increment(intKeys(i))
      longIntMap.increment(longKeys(i))
      i += 1
    }
    interner = InternMap.concurrent[String](N)
    i = 0
    while (i < N) {
      interner.intern(strKeys(i))
      i += 1
    }
  }

  // ---- build (insert) path ----

  @Threads(1)
  @Benchmark
  def buildRefIntHashMap(bh: Blackhole): Unit = {
    val m = new RefIntHashMap[String]
    var i = 0
    while (i < N) { m.increment(strKeys(i)); i += 1 }
    bh.consume(m)
  }

  @Threads(1)
  @Benchmark
  def buildIntRefHashMap(bh: Blackhole): Unit = {
    val m = new IntRefHashMap[String](Int.MinValue)
    var i = 0
    while (i < N) { m.put(intKeys(i), "v"); i += 1 }
    bh.consume(m)
  }

  @Threads(1)
  @Benchmark
  def buildIntIntHashMap(bh: Blackhole): Unit = {
    val m = new IntIntHashMap(-1)
    var i = 0
    while (i < N) { m.increment(intKeys(i)); i += 1 }
    bh.consume(m)
  }

  @Threads(1)
  @Benchmark
  def buildLongHashSet(bh: Blackhole): Unit = {
    val s = new LongHashSet(Long.MinValue)
    var i = 0
    while (i < N) { s.add(longKeys(i)); i += 1 }
    bh.consume(s)
  }

  @Threads(1)
  @Benchmark
  def buildLongIntHashMap(bh: Blackhole): Unit = {
    val m = new LongIntHashMap(-1L)
    var i = 0
    while (i < N) { m.increment(longKeys(i)); i += 1 }
    bh.consume(m)
  }

  @Threads(1)
  @Benchmark
  def buildIntHashSet(bh: Blackhole): Unit = {
    val s = new IntHashSet(Int.MinValue)
    var i = 0
    while (i < N) { s.add(intKeys(i)); i += 1 }
    bh.consume(s)
  }

  @Threads(1)
  @Benchmark
  def buildInterner(bh: Blackhole): Unit = {
    val in = InternMap.concurrent[String](N)
    var i = 0
    while (i < N) { bh.consume(in.intern(strKeys(i))); i += 1 }
  }

  // ---- query (get) path: half hits, half misses ----

  @Threads(1)
  @Benchmark
  def queryRefIntHashMap(bh: Blackhole): Unit = {
    var i = 0
    while (i < N) {
      bh.consume(refIntMap.get(strKeys(i), -1))
      bh.consume(refIntMap.get(strMiss(i), -1))
      i += 1
    }
  }

  @Threads(1)
  @Benchmark
  def queryIntRefHashMap(bh: Blackhole): Unit = {
    var i = 0
    while (i < N) {
      bh.consume(intRefMap.get(intKeys(i)))
      bh.consume(intRefMap.get(intMiss(i)))
      i += 1
    }
  }

  @Threads(1)
  @Benchmark
  def queryIntIntHashMap(bh: Blackhole): Unit = {
    var i = 0
    while (i < N) {
      bh.consume(intIntMap.get(intKeys(i), -1))
      bh.consume(intIntMap.get(intMiss(i), -1))
      i += 1
    }
  }

  @Threads(1)
  @Benchmark
  def queryLongIntHashMap(bh: Blackhole): Unit = {
    var i = 0
    while (i < N) {
      bh.consume(longIntMap.get(longKeys(i), -1))
      i += 1
    }
  }

  @Threads(1)
  @Benchmark
  def queryInterner(bh: Blackhole): Unit = {
    var i = 0
    while (i < N) {
      bh.consume(interner.intern(strKeys(i)))
      i += 1
    }
  }
}

object OpenHashMapReduction {
  private val N = 50000
}
