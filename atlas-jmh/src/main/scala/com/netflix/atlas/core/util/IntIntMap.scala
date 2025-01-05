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
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

import scala.util.Random

/**
  * Sanity check for integer hash set.
  *
  * ```
  * > run -wi 10 -i 10 -f1 -t1 .*IntIntMap.*
  * ...
  * load factor of 0.7
  *
  * [info] Benchmark                        Mode  Cnt      Score     Error  Units
  * [info] IntIntMap.testIntIntHashMap800  thrpt   10  10374.640 ± 782.568  ops/s
  * [info] IntIntMap.testIntIntHashMap8k   thrpt   10    589.537 ±   8.124  ops/s
  * [info] IntIntMap.testJavaHashMap800    thrpt   10   4051.768 ± 499.362  ops/s
  * [info] IntIntMap.testJavaHashMap8k     thrpt   10   2846.103 ±  14.858  ops/s
  *
  * load factor of 0.5
  *
  * [info] Benchmark                        Mode  Cnt      Score     Error  Units
  * [info] IntIntMap.testIntIntHashMap800  thrpt   10  11713.871 ± 271.158  ops/s
  * [info] IntIntMap.testIntIntHashMap8k   thrpt   10   3963.805 ±  59.359  ops/s
  * [info] IntIntMap.testJavaHashMap800    thrpt   10   4371.380 ±  93.346  ops/s
  * [info] IntIntMap.testJavaHashMap8k     thrpt   10   2709.591 ±  71.376  ops/s
  * ```
  */
@State(Scope.Thread)
class IntIntMap {

  private val values800 = (0 until 10000).map(_ => math.abs(Random.nextInt(800))).toArray
  private val values8k = (0 until 10000).map(_ => math.abs(Random.nextInt(8000))).toArray

  @Threads(1)
  @Benchmark
  def testIntIntHashMap800(bh: Blackhole): Unit = {
    val map = new IntIntHashMap(-1, 10)
    var i = 0
    while (i < values800.length) {
      map.increment(values800(i))
      i += 1
    }
    bh.consume(map)
  }

  @Threads(1)
  @Benchmark
  def testJavaHashMap800(bh: Blackhole): Unit = {
    val map = new java.util.HashMap[Int, Int](10)
    var i = 0
    while (i < values800.length) {
      map.compute(values800(i), (_, v) => v + 1)
      i += 1
    }
    bh.consume(map)
  }

  @Threads(1)
  @Benchmark
  def testIntIntHashMap8k(bh: Blackhole): Unit = {
    val map = new IntIntHashMap(-1, 10)
    var i = 0
    while (i < values8k.length) {
      map.increment(values8k(i))
      i += 1
    }
    bh.consume(map)
  }

  @Threads(1)
  @Benchmark
  def testJavaHashMap8k(bh: Blackhole): Unit = {
    val map = new java.util.HashMap[Int, Int](10)
    var i = 0
    while (i < values8k.length) {
      map.compute(values8k(i), (_, v) => v + 1)
      i += 1
    }
    bh.consume(map)
  }
}
