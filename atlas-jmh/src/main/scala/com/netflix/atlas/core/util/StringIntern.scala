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

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import java.util.UUID

/**
  * ```
  * > jmh:run -wi 10 -i 10 -f1 -t1 .*StringIntern.*
  * ```
  *
  * Results:
  *
  * ```
  * Benchmark           Mode  Cnt     Score     Error  Units
  * caffeineIntern100  thrpt   10  5030.208 ± 179.907  ops/s
  * caffeineIntern50   thrpt   10   689.302 ±  36.348  ops/s
  * customIntern       thrpt   10  1249.873 ±  32.805  ops/s
  * javaIntern         thrpt   10   664.655 ±  40.109  ops/s
  * ```
  */
@State(Scope.Thread)
class StringIntern {

  private val n = 10_000

  private var javaInterner: Interner[String] = _
  private var caffeineInterner50: Interner[String] = _
  private var caffeineInterner100: Interner[String] = _
  private var customInterner: Interner[String] = _

  private var strings: Array[String] = _

  @Setup
  def setup(): Unit = {
    javaInterner = StringInterner
    caffeineInterner50 = new CaffeineInterner[String](n / 2)
    caffeineInterner100 = new CaffeineInterner[String](n * 5)
    customInterner = InternMap.concurrent[String](n)

    strings = new Array(n)
    var i = 0
    while (i < n) {
      strings(i) = UUID.randomUUID().toString
      i += 1
    }
  }

  private def internStrings(bh: Blackhole, interner: Interner[String]): Unit = {
    var i = 0
    while (i < n) {
      bh.consume(interner.intern(strings(i)))
      i += 1
    }
  }

  @Benchmark
  def javaIntern(bh: Blackhole): Unit = {
    internStrings(bh, javaInterner)
  }

  // Interner max set to only handle 50% of string values
  @Benchmark
  def caffeineIntern50(bh: Blackhole): Unit = {
    internStrings(bh, caffeineInterner50)
  }

  // Interner max set to only handle 100% of string values
  @Benchmark
  def caffeineIntern100(bh: Blackhole): Unit = {
    internStrings(bh, caffeineInterner100)
  }

  @Benchmark
  def customIntern(bh: Blackhole): Unit = {
    internStrings(bh, customInterner)
  }
}
