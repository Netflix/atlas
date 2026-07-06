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
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
  * Lock-contention test for the shared `ConcurrentInternMap`. All threads hit a pre-populated,
  * shared interner so the working-set size is stable; the difference between the benchmarks is
  * purely how many segment locks each op takes:
  *
  *  - `getOnly`       — one `get` (1 lock): the steady-state hit path.
  *  - `getThenIntern` — a `get` then an `intern` (2 locks): models #1929's miss path lock count.
  *  - `getOrIntern`   — the fused single chain walk (1 lock).
  *
  * Sweeping `concurrency` (the segment count) at a fixed thread count shows whether the shared
  * lock is a bottleneck and how many segments are needed to relieve it. Run at several `-t`:
  *
  * > jmh:run -wi 5 -i 10 -f1 -t8 .*ConcurrentInternBench.*
  * > jmh:run -wi 5 -i 10 -f1 -t1 .*ConcurrentInternBench.*
  */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class ConcurrentInternBench {

  import ConcurrentInternBench.*

  // Segment count of the shared interner (the `concurrencyLevel`). 8 = r5.2xlarge vCPUs.
  @Param(Array("8", "16", "32", "64"))
  var concurrency: Int = 0

  private final val numKeys = 50000
  private var keys: Array[SortedTagMap] = _
  private var hashes: Array[Int] = _
  private var interner: InternMap[Map[String, String]] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    keys = Array.tabulate(numKeys) { i =>
      SortedTagMap(
        "nf.app"     -> "atlas_backend",
        "nf.cluster" -> "atlas_backend-dev",
        "nf.asg"     -> "atlas_backend-dev-v001",
        "nf.region"  -> "us-east-1",
        "nf.zone"    -> "us-east-1e",
        "nf.node"    -> "i-123456789",
        "name"       -> "jvm.gc.pause",
        "statistic"  -> "totalTime",
        "id"         -> i.toString
      )
    }
    hashes = keys.map(_.hashCode)
    interner =
      InternMap.concurrent[Map[String, String]](numKeys * 2, concurrencyLevel = concurrency)
    var i = 0
    while (i < numKeys) { interner.intern(keys(i), 0L); i += 1 }
  }

  @Benchmark
  def getOnly(ts: ThreadState, bh: Blackhole): Unit = {
    val i = ts.next(numKeys)
    ts.key = keys(i)
    bh.consume(interner.get(hashes(i), ts.isEqual, 0L))
  }

  @Benchmark
  def getThenIntern(ts: ThreadState, bh: Blackhole): Unit = {
    val i = ts.next(numKeys)
    val k = keys(i)
    ts.key = k
    interner.get(hashes(i), ts.isEqual, 0L)
    bh.consume(interner.intern(k, 0L))
  }

  @Benchmark
  def getOrIntern(ts: ThreadState, bh: Blackhole): Unit = {
    val i = ts.next(numKeys)
    ts.key = keys(i)
    bh.consume(interner.getOrIntern(hashes(i), ts.probe, 0L))
  }
}

object ConcurrentInternBench {

  /** Per-thread cursor and reusable predicates so the loop body allocates nothing. */
  @State(Scope.Thread)
  class ThreadState {

    var cursor: Int = 0
    var key: SortedTagMap = _

    // Reusable predicate for `get`; reads the current `key` each call (still boxes the Boolean
    // through the Function1 bridge, matching the #1929 probe, but allocates no per-call closure).
    val isEqual: Map[String, String] => Boolean = m => m.equals(key)

    // Reusable probe for `getOrIntern`; `create` is never reached here because every op hits.
    val probe: InternMap.InternProbe[Map[String, String]] =
      new InternMap.InternProbe[Map[String, String]] {

        def matches(m: Map[String, String]): Boolean = m.equals(key)
        def create(): Map[String, String] = key
      }

    def next(n: Int): Int = {
      val i = cursor
      cursor = if (i + 1 < n) i + 1 else 0
      i
    }
  }
}
