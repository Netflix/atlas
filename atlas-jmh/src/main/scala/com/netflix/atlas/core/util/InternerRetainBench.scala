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

import java.util.UUID

/**
  * Compares the cost of `OpenHashInternMap.retain` via in-place backward-shift deletion vs the
  * full rehash, in the case that dominates production: the index rebuild calls `retain` every
  * `rebuild-frequency` (~40s) but entries expire on the data-window timescale (~days), so almost
  * every retain drops nothing. Here `keep = _ => true` (nothing expired): backward-shift should be
  * a cheap O(n) scan with no allocation, while the rehash re-inserts every entry into fresh arrays.
  *
  * Both operations leave the interner contents unchanged for the keep-all case, so a single
  * pre-built interner is reused across invocations.
  *
  * ```
  * > jmh:run -wi 5 -i 10 -f1 -t1 -prof gc .*InternerRetainBench.*
  * ```
  */
@State(Scope.Thread)
class InternerRetainBench {

  private val n = 1_000_000
  private var interner: OpenHashInternMap[String] = _

  @Setup
  def setup(): Unit = {
    interner = new OpenHashInternMap[String](n)
    var i = 0
    while (i < n) {
      interner.intern(UUID.randomUUID().toString)
      i += 1
    }
  }

  @Benchmark
  def retainBackwardShift(bh: Blackhole): Unit = {
    interner.retain(_ => true)
    bh.consume(interner.size)
  }

  @Benchmark
  def retainRehash(bh: Blackhole): Unit = {
    interner.retainByRehash(_ => true)
    bh.consume(interner.size)
  }
}
