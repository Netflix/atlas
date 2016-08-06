/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.SmallHashMap
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Check performance of TaggedItem.computeId. It is in the hot path when processing
  * new metrics that come in via publish.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*ComputeId.*
  * ...
  * [info] Benchmark                            Mode  Cnt       Score       Error  Units
  * [info] ComputeId.computeIdSmallTagMap      thrpt   10  252703.940 ± 11424.615  ops/s
  * [info] ComputeId.computeIdTagMap           thrpt   10  248390.915 ± 10755.616  ops/s
  * ```
  */
@State(Scope.Thread)
class ComputeId {

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
  def computeIdTagMap(bh: Blackhole): Unit = {
    bh.consume(TaggedItem.computeId(tagMap))
  }

  @Threads(1)
  @Benchmark
  def computeIdSmallTagMap(bh: Blackhole): Unit = {
    bh.consume(TaggedItem.computeId(smallTagMap))
  }
}
