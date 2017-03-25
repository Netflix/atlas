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
  * Check performance of creating a copy of the map when adding/removing a single pair.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*SmallHashMapModify.*
  * ...
  * [info] Benchmark                           Mode  Cnt        Score       Error  Units
  * [info] SmallHashMapModify.addPair         thrpt   10  1637470.772 ± 29948.315  ops/s
  * [info] SmallHashMapModify.removePair      thrpt   10  2520939.662 ± 32320.759  ops/s
  * ```
  */
@State(Scope.Thread)
class SmallHashMapModify {

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
  def addPair(bh: Blackhole): Unit = {
    bh.consume(smallTagMap + ("foo" -> "bar"))
  }

  @Threads(1)
  @Benchmark
  def removePair(bh: Blackhole): Unit = {
    bh.consume(smallTagMap - "nf.ami")
  }
}
