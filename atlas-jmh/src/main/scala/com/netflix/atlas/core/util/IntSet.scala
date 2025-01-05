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
  * > run -wi 10 -i 10 -f1 -t1 .*IntSet.*
  * ...
  * [info] Benchmark                    Mode  Cnt       Score       Error  Units
  * [info] IntSet.testIntHashSet       thrpt   10  478541.525 ± 29615.965  ops/s
  * [info] IntSet.testIntHashSet10k    thrpt   10    1531.614 ±    73.903  ops/s
  * [info] IntSet.testJavaHashSet      thrpt   10  376161.309 ±  5242.341  ops/s
  * [info] IntSet.testJavaHashSet10k   thrpt   10    2169.164 ±   126.964  ops/s
  * [info] IntSet.testTroveHashSet     thrpt   10  416539.087 ± 29554.649  ops/s
  * [info] IntSet.testTroveHashSet10k  thrpt   10    1813.617 ±    30.182  ops/s
  * ```
  */
@State(Scope.Thread)
class IntSet {

  private val values100 = (0 until 100).map(_ => math.abs(Random.nextInt())).toArray
  private val values10k = (0 until 10000).map(_ => math.abs(Random.nextInt())).toArray

  @Threads(1)
  @Benchmark
  def testIntHashSet(bh: Blackhole): Unit = {
    val set = new IntHashSet(-1, 10)
    var i = 0
    while (i < values100.length) {
      set.add(values100(i))
      i += 1
    }
    bh.consume(set)
  }

  @Threads(1)
  @Benchmark
  def testJavaHashSet(bh: Blackhole): Unit = {
    val set = new java.util.HashSet[Int](10)
    var i = 0
    while (i < values100.length) {
      set.add(values100(i))
      i += 1
    }
    bh.consume(set)
  }

  @Threads(1)
  @Benchmark
  def testIntHashSet10k(bh: Blackhole): Unit = {
    val set = new IntHashSet(-1, 10)
    var i = 0
    while (i < values10k.length) {
      set.add(values10k(i))
      i += 1
    }
    bh.consume(set)
  }

  @Threads(1)
  @Benchmark
  def testJavaHashSet10k(bh: Blackhole): Unit = {
    val set = new java.util.HashSet[Int](10)
    var i = 0
    while (i < values10k.length) {
      set.add(values10k(i))
      i += 1
    }
    bh.consume(set)
  }
}
