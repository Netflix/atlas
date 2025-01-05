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

import com.netflix.atlas.core.util.SortedTagMap
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Check performance of special case in KeyValueQuery to use SmallHashMap.getOrNull when
  * possible instead of Map.get. This avoids an allocation for the Option and a little bit
  * of overhead for the lambda invocation. For tight loops such as checking the query for a
  * high volume streaming path it provides a noticeable benefit.
  *
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*KeyValueQuery.*
  * ```
  */
@State(Scope.Thread)
class KeyValueQuery {

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

  private val smallTagMap = SortedTagMap(tagMap)

  private val query = Query.And(
    Query.Equal("nf.app", "atlas_backend"),
    Query.And(
      Query.Equal("nf.stack", "dev"),
      Query.And(
        Query.Equal("name", "jvm.gc.pause"),
        Query.Equal("cause", "Allocation_Failure")
      )
    )
  )

  @Threads(1)
  @Benchmark
  def checkMap(bh: Blackhole): Unit = {
    bh.consume(query.matches(tagMap))
  }

  @Threads(1)
  @Benchmark
  def checkSmallMap(bh: Blackhole): Unit = {
    bh.consume(query.matches(smallTagMap))
  }
}
