/*
 * Copyright 2014-2017 Netflix, Inc.
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
  * Check performance of comparing small hash maps for equality.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*SmallHashMapEquals.*
  * ...
  * [info] Benchmark                     Mode  Cnt          Score          Error  Units
  *
  * [info] currentEquals                thrpt   10   47004215.059 ±  2291847.719  ops/s
  * [info] inheritedEquals              thrpt   10    2134457.383 ±   111821.520  ops/s
  * [info] dataEquals                   thrpt   10   51326103.645 ±   758297.787  ops/s
  * [info] selfEquals                   thrpt   10  351279563.043 ± 18578115.816  ops/s
  *
  * [info] currentEqualsNot             thrpt   10  273893522.221 ±  7980051.600  ops/s
  * [info] inheritedEqualsNot           thrpt   10    2341207.187 ±   206356.584  ops/s
  * [info] dataEqualsNot                thrpt   10   32392263.165 ±  1289262.059  ops/s
  *
  * [info] currentEqualHashCodes        thrpt   10   14601802.119 ±   360902.793  ops/s
  * [info] inheritedEqualHashCodes      thrpt   10     483515.784 ±    10044.781  ops/s
  * ```
  */
@State(Scope.Thread)
class SmallHashMapEquals {

  private val tagMap = Map(
    "nf.app"     -> "atlas_backend",
    "nf.cluster" -> "atlas_backend-dev",
    "nf.asg"     -> "atlas_backend-dev-v001",
    "nf.stack"   -> "dev",
    "nf.region"  -> "us-east-1",
    "nf.zone"    -> "us-east-1e",
    "nf.node"    -> "i-123456789",
    "nf.ami"     -> "ami-987654321",
    "nf.vmtype"  -> "r3.2xlarge",
    "name"       -> "jvm.gc.pause",
    "cause"      -> "Allocation_Failure",
    "action"     -> "end_of_major_GC",
    "statistic"  -> "totalTime"
  )

  private val smallTagMap1 = SmallHashMap(tagMap)
  private val smallTagMap2 = SmallHashMap(tagMap)

  private val smallTagMap3 = SmallHashMap(tagMap + ("nf.node" -> "i-987654321"))

  private val (randomMap1, randomMap2) = {
    val n = Random.nextInt(50)
    val data = (0 until n).map { _ =>
      val v = Random.nextInt()
      v.toString -> v.toString
    }
    val m1 = SmallHashMap(data)
    val m2 = SmallHashMap(Random.shuffle(data))
    m1 -> m2
  }

  @Threads(1)
  @Benchmark
  def selfEquals(bh: Blackhole): Unit = {
    bh.consume(smallTagMap1.equals(smallTagMap1))
  }

  @Threads(1)
  @Benchmark
  def currentEquals(bh: Blackhole): Unit = {
    bh.consume(smallTagMap1.equals(smallTagMap2))
  }

  @Threads(1)
  @Benchmark
  def inheritedEquals(bh: Blackhole): Unit = {
    bh.consume(smallTagMap1.superEquals(smallTagMap2))
  }

  @Threads(1)
  @Benchmark
  def dataEquals(bh: Blackhole): Unit = {
    bh.consume(smallTagMap1.dataEquals(smallTagMap2))
  }

  @Threads(1)
  @Benchmark
  def currentEqualsNot(bh: Blackhole): Unit = {
    bh.consume(smallTagMap1.equals(smallTagMap3))
  }

  @Threads(1)
  @Benchmark
  def inheritedEqualsNot(bh: Blackhole): Unit = {
    bh.consume(smallTagMap1.superEquals(smallTagMap3))
  }

  @Threads(1)
  @Benchmark
  def dataEqualsNot(bh: Blackhole): Unit = {
    bh.consume(smallTagMap1.dataEquals(smallTagMap3))
  }

  @Threads(1)
  @Benchmark
  def currentEqualHashCodes(bh: Blackhole): Unit = {
    bh.consume(randomMap1.equals(randomMap2))
  }

  @Threads(1)
  @Benchmark
  def inheritedEqualHashCodes(bh: Blackhole): Unit = {
    bh.consume(randomMap1.superEquals(randomMap2))
  }
}
