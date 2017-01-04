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

/**
  * Check performance of computing the hash code for a small hash map. Note it will cache after
  * the first run, see test with caching. However, the murmur3* tests are more interesting as
  * they indicate the performance for the initial computation which is often in the critical
  * path for new tag maps.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*SmallHashMapHashCode.*
  * ...
  * [info] Benchmark              Mode  Cnt          Score          Error  Units
  * [info] computeHashCode       thrpt   10   33962020.269 ±   883664.164  ops/s
  * [info] currentHashCode       thrpt   10  360180789.347 ± 10164654.707  ops/s
  * [info] murmur3arrayHash      thrpt   10   13861013.249 ±  3160191.522  ops/s
  * [info] murmur3mapHash        thrpt   10    4067194.458 ±    78185.171  ops/s
  * ```
  */
@State(Scope.Thread)
class SmallHashMapHashCode {

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

  private val smallTagMap = SmallHashMap(tagMap)

  @Threads(1)
  @Benchmark
  def currentHashCode(bh: Blackhole): Unit = {
    bh.consume(smallTagMap.hashCode)
  }

  @Threads(1)
  @Benchmark
  def computeHashCode(bh: Blackhole): Unit = {
    bh.consume(smallTagMap.computeHashCode)
  }

  @Threads(1)
  @Benchmark
  def murmur3mapHash(bh: Blackhole): Unit = {
    bh.consume(scala.util.hashing.MurmurHash3.mapHash(smallTagMap))
  }

  @Threads(1)
  @Benchmark
  def murmur3arrayHash(bh: Blackhole): Unit = {
    bh.consume(scala.util.hashing.MurmurHash3.arrayHash(smallTagMap.data))
  }
}
