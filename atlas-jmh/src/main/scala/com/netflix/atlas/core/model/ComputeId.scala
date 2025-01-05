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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.SortedTagMap
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
  * > jmh:run -prof gc -prof stack -wi 5 -i 5 -f1 -t1 .*ComputeId.*
  * ...
  * Benchmark                             Mode  Cnt       Score       Error   Units
  * computeIdNaive                       thrpt    5  326439.391 ± 30418.108   ops/s
  * computeIdSmallTagMap                 thrpt    5  512260.825 ± 36192.269   ops/s
  * computeIdSortedTagMap                thrpt    5  578262.359 ± 70766.713   ops/s
  * computeIdTagMap                      thrpt    5  519393.255 ± 16961.856   ops/s
  *
  * Benchmark                             Mode  Cnt       Score       Error   Units
  * computeIdNaive                       alloc    5    8408.000 ±     0.001    B/op
  * computeIdSmallTagMap                 alloc    5      64.000 ±     0.001    B/op
  * computeIdSortedTagMap                alloc    5      64.000 ±     0.001    B/op
  * computeIdTagMap                      alloc    5      80.000 ±     0.001    B/op
  * ```
  *
  * Note, the naive method is mostly problematic in terms of allocated data, not
  * throughput. For this benchmark it allocates over 25GB for the top 5 objects compared
  * to 5GB to 6GB when using computeId.
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

  private val sortedTagMap = SortedTagMap(tagMap)

  @Threads(1)
  @Benchmark
  def computeIdNaive(bh: Blackhole): Unit = {
    val sorted = tagMap.toList.sortWith(_._1 < _._1).map(t => t._1 + "=" + t._2)
    val id = Hash.sha1(sorted.mkString(","))
    bh.consume(id)
  }

  @Threads(1)
  @Benchmark
  def computeIdTagMap(bh: Blackhole): Unit = {
    bh.consume(TaggedItem.computeId(tagMap))
  }

  @Threads(1)
  @Benchmark
  def computeIdSortedTagMap(bh: Blackhole): Unit = {
    bh.consume(TaggedItem.computeId(sortedTagMap))
  }
}
