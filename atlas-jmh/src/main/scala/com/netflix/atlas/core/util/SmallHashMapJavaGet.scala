/*
 * Copyright 2014-2023 Netflix, Inc.
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
  * Check the overhead of java wrapper for use-cases that perform a lot of get
  * calls.
  *
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*SmallHashMapJavaGet.*
  * ...
  * [info] Benchmark               Mode  Cnt         Score         Error  Units
  * [info] customGetFound         thrpt   10  94270900.203 ± 5825997.538  ops/s
  * [info] customGetNotFound      thrpt   10   6799704.339 ±  462769.738  ops/s
  * [info] scalaGetFound          thrpt   10  85325015.251 ± 4367808.653  ops/s
  * [info] scalaGetNotFound       thrpt   10   6385962.734 ±  318520.923  ops/s
  * ```
  */
@State(Scope.Thread)
class SmallHashMapJavaGet {

  import scala.jdk.CollectionConverters.*

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

  @Benchmark
  def scalaGetFound(bh: Blackhole): Unit = {
    bh.consume(scalaWrapper.get("name"))
  }

  @Benchmark
  def customGetFound(bh: Blackhole): Unit = {
    bh.consume(customWrapper.get("name"))
  }

  @Benchmark
  def scalaGetNotFound(bh: Blackhole): Unit = {
    bh.consume(scalaWrapper.get("foo"))
  }

  @Benchmark
  def customGetNotFound(bh: Blackhole): Unit = {
    bh.consume(customWrapper.get("foo"))
  }
}
