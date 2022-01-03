/*
 * Copyright 2014-2022 Netflix, Inc.
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
import org.openjdk.jmh.infra.Blackhole

/**
  * Check the overhead of java wrapper for use-cases that iterate over the entry set.
  *
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*SmallHashMapEntrySet.*
  * ...
  * Benchmark                      Mode  Cnt         Score        Error   Units
  * customEntrySet                thrpt   10  13480190.254 ± 351370.866   ops/s
  * scalaEntrySet                 thrpt   10   7782130.178 ± 514660.491   ops/s
  *
  * customEntrySet   gc.alloc.rate.norm   10        24.000 ±      0.001    B/op
  * scalaEntrySet    gc.alloc.rate.norm   10       272.000 ±      0.001    B/op
  * ```
  */
@State(Scope.Thread)
class SmallHashMapEntrySet {

  import scala.jdk.CollectionConverters._

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
  private val scalaWrapper = smallTagMap.asJava
  private val customWrapper = smallTagMap.asJavaMap

  private def traverseMap(bh: Blackhole, m: java.util.Map[String, String]): Unit = {
    val it = m.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      bh.consume(entry.getKey)
      bh.consume(entry.getValue)
    }
  }

  @Benchmark
  def scalaEntrySet(bh: Blackhole): Unit = {
    traverseMap(bh, scalaWrapper)
  }

  @Benchmark
  def customEntrySet(bh: Blackhole): Unit = {
    traverseMap(bh, customWrapper)
  }
}
