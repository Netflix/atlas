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
  * There was an old suggestion that max via bit manip would be faster and avoid branch instructions. That
  * doesn't appear to be the case:
  *
  * ```
  * > run -wi 10 -i 10 -f1 -t1 .*MathMax.*
  * ...
  * [info] Benchmark                                  Mode  Samples           Score   Score error  Units
  * [info] c.n.a.c.u.MathMax.testMaxWithBitManip     thrpt       10   849801537.629  13395055.478  ops/s
  * [info] c.n.a.c.u.MathMax.testMaxWithIf           thrpt       10  1015950530.655  14758001.309  ops/s
  * [info] c.n.a.c.u.MathMax.testMaxWithScalaMath    thrpt       10  1021526747.880   8691594.381  ops/s
  * ```
  */
@State(Scope.Thread)
class MathMax {

  private val v1 = Random.nextInt()
  private val v2 = Random.nextInt()

  private def maxWithBitManip(v1: Int, v2: Int): Int = {
    val diff = v1 - v2
    v1 - (diff & (diff >> 31))
  }

  private def maxWithIf(v1: Int, v2: Int): Int = {
    if (v1 > v2) v1 else v2
  }

  @Threads(1)
  @Benchmark
  def testMaxWithIf(bh: Blackhole): Unit = {
    bh.consume(maxWithIf(v1, v2))
  }

  @Threads(1)
  @Benchmark
  def testMaxWithBitManip(bh: Blackhole): Unit = {
    bh.consume(maxWithBitManip(v1, v2))
  }

  @Threads(1)
  @Benchmark
  def testMaxWithScalaMath(bh: Blackhole): Unit = {
    bh.consume(scala.math.max(v1, v2))
  }
}
