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
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Compares interner strategies on publish-shaped tag maps: the pre-#1929 single `intern`, the
  * #1929 two-step `get` + `intern` (probe, then intern on a miss), and the fused `getOrIntern`
  * (one chain walk). Run with `-prof gc` and `-prof stack` to see where the miss-path cost goes.
  *
  * The `miss*` benchmarks intern N distinct series into a fresh interner each invocation (models a
  * high-cardinality / cold box where the probe rarely hits). The `hit*` benchmarks re-look-up a
  * pre-populated interner (models steady state where the probe should hit).
  *
  * > jmh:run -prof gc -wi 5 -i 10 -f1 -t1 .*GetOrInternBench.*
  */
@State(Scope.Thread)
class GetOrInternBench {

  // Number of distinct series interned per invocation.
  @Param(Array("10000"))
  var n: Int = 0

  // "shared" = realistic publish maps that share a long common nf.* prefix and differ in one tag
  // value (stresses equals, which must scan the shared prefix before reaching the difference).
  // "distinct" = every tag differs (control: cheap equals, well-spread hashes).
  @Param(Array("shared", "distinct"))
  var shape: String = ""

  private var keys: Array[SortedTagMap] = _
  private var hashes: Array[Int] = _
  private var warm: OpenHashInternMap[Map[String, String]] = _

  private def buildKey(i: Int): SortedTagMap = shape match {
    case "shared" =>
      SortedTagMap(
        "nf.app"     -> "atlas_backend",
        "nf.cluster" -> "atlas_backend-dev",
        "nf.asg"     -> "atlas_backend-dev-v001",
        "nf.stack"   -> "dev",
        "nf.region"  -> "us-east-1",
        "nf.zone"    -> "us-east-1e",
        "nf.node"    -> "i-123456789",
        "nf.vmtype"  -> "r3.2xlarge",
        "name"       -> "jvm.gc.pause",
        "statistic"  -> "totalTime",
        "id"         -> i.toString
      )
    case _ =>
      SortedTagMap((0 until 11).map(j => s"k$j-$i" -> s"v$j-$i")*)
  }

  @Setup(Level.Trial)
  def setup(): Unit = {
    keys = Array.tabulate(n)(buildKey)
    hashes = keys.map(_.hashCode)
    // Pre-populated interner for the hit benchmarks.
    warm = new OpenHashInternMap[Map[String, String]](n * 2)
    var i = 0
    while (i < n) { warm.intern(keys(i), 0L); i += 1 }
  }

  // Reusable probe that confirms a candidate by content and, on a miss, returns the prebuilt key.
  private final class KeyProbe extends InternMap.InternProbe[Map[String, String]] {

    var key: SortedTagMap = _
    def matches(m: Map[String, String]): Boolean = m.equals(key)
    def create(): Map[String, String] = key
  }

  // Pre-#1929: build the map (already built here) and intern it; dedup happens inside intern.
  @Benchmark
  def missIntern(bh: Blackhole): Unit = {
    val m = new OpenHashInternMap[Map[String, String]](16)
    var i = 0
    while (i < n) { bh.consume(m.intern(keys(i), 0L)); i += 1 }
  }

  // #1929: probe with get, then intern on a miss (two chain walks on a miss).
  @Benchmark
  def missTwoStep(bh: Blackhole): Unit = {
    val m = new OpenHashInternMap[Map[String, String]](16)
    var i = 0
    while (i < n) {
      val k = keys(i)
      val existing = m.get(hashes(i), candidate => candidate.equals(k), 0L)
      bh.consume(if (existing != null) existing else m.intern(k, 0L))
      i += 1
    }
  }

  // Fused: a single chain walk.
  @Benchmark
  def missGetOrIntern(bh: Blackhole): Unit = {
    val m = new OpenHashInternMap[Map[String, String]](16)
    val probe = new KeyProbe
    var i = 0
    while (i < n) {
      probe.key = keys(i)
      bh.consume(m.getOrIntern(hashes(i), probe, 0L))
      i += 1
    }
  }

  // Steady state: every lookup hits the pre-populated interner.
  @Benchmark
  def hitTwoStep(bh: Blackhole): Unit = {
    var i = 0
    while (i < n) {
      val k = keys(i)
      bh.consume(warm.get(hashes(i), candidate => candidate.equals(k), 0L))
      i += 1
    }
  }

  @Benchmark
  def hitGetOrIntern(bh: Blackhole): Unit = {
    val probe = new KeyProbe
    var i = 0
    while (i < n) {
      probe.key = keys(i)
      bh.consume(warm.getOrIntern(hashes(i), probe, 0L))
      i += 1
    }
  }
}
