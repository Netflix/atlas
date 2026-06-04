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
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Iterating an `IntRefHashMap` via the specialized `Consumer` SAM avoids boxing the
  * int key, which a `Function2[Int, T, Unit]` callback would do (no specialized
  * `apply` exists when one parameter is a reference type, so the generic
  * `apply(Object, Object)` boxes the key). This is the hot path behind
  * `RoaringTagIndex` pattern queries (`strPattern`).
  *
  * `consumer` and `boxedFunction2` run the identical loop over the same data and
  * differ only in the callback type, isolating the boxing cost. `mapForeach` uses the
  * real `map.foreach` (now SAM).
  *
  * ```
  * > jmh:run -wi 5 -i 5 -f1 -t1 -prof gc .*IntRefForeach.*
  * ```
  *
  * Observed (size=1000, -wi 5 -i 8 -f1; absolute numbers vary by machine/JDK):
  * ```
  * boxedFunction2   2.49M ops/s
  * consumer         4.14M ops/s   (~1.66x: no box/unbox, no generic apply)
  * mapForeach       0.88M ops/s   (real sparse-table traversal; box-free)
  * ```
  * Note: in this tight monomorphic loop the JIT scalarizes the boxing, so
  * gc.alloc.rate.norm is ~0 for all three; the throughput gap is the on-CPU box/unbox
  * cost (the 5% self-time `boxToInteger` seen in CPU profiles). At polymorphic
  * production call sites escape analysis often fails and the box also allocates, so
  * removing it at the bytecode level (the SAM) is the robust fix.
  */
@State(Scope.Thread)
class IntRefForeach {

  private val size = 1000

  private var map: IntRefHashMap[String] = _

  // Parallel arrays holding the same entries, so the boxing/non-boxing callbacks
  // traverse identical data and differ only in the callback type.
  private var keys: Array[Int] = _
  private var values: Array[String] = _

  @Setup
  def setup(): Unit = {
    map = new IntRefHashMap[String](noData = Int.MinValue, capacity = 2 * size)
    keys = new Array[Int](size)
    values = new Array[String](size)
    var i = 0
    while (i < size) {
      // Keys above the Integer cache (>127) so boxing actually allocates.
      val k = 1000 + i
      val v = k.toString
      map.put(k, v)
      keys(i) = k
      values(i) = v
      i += 1
    }
  }

  // Mirrors the pre-change signature: a Function2 with a reference-typed parameter,
  // which forces the generic apply(Object, Object) and boxes the int key.
  private def boxedLoop(f: (Int, String) => Unit): Unit = {
    var i = 0
    while (i < keys.length) {
      f(keys(i), values(i))
      i += 1
    }
  }

  private def consumerLoop(f: IntRefHashMap.Consumer[String]): Unit = {
    var i = 0
    while (i < keys.length) {
      f.accept(keys(i), values(i))
      i += 1
    }
  }

  @Benchmark
  def boxedFunction2(bh: Blackhole): Unit = {
    boxedLoop((k, v) => { bh.consume(k); bh.consume(v) })
  }

  @Benchmark
  def consumer(bh: Blackhole): Unit = {
    consumerLoop((k, v) => { bh.consume(k); bh.consume(v) })
  }

  @Benchmark
  def mapForeach(bh: Blackhole): Unit = {
    map.foreach((k, v) => { bh.consume(k); bh.consume(v) })
  }
}
