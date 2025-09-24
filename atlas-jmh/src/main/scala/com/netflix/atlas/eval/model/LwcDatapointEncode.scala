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
package com.netflix.atlas.eval.model

import java.io.StringWriter

import com.netflix.atlas.json.Json
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*LwcDatapointEncode.*
  * ```
  *
  * Throughput:
  *
  * ```
  * Benchmark                          Mode  Cnt       Score       Error   Units
  * customEncode                      thrpt   10  778092.834 ± 39878.875   ops/s
  * defaultEncode                     thrpt   10  679355.101 ± 27340.720   ops/s
  * ```
  *
  * Allocations:
  *
  * ```
  * Benchmark                          Mode  Cnt       Score       Error   Units
  * customEncode         gc.alloc.rate.norm   10    2136.000 ±     0.001    B/op
  * defaultEncode        gc.alloc.rate.norm   10    3000.000 ±     0.001    B/op
  * ```
  **/
@State(Scope.Thread)
class LwcDatapointEncode {

  private val tags = Map(
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

  private val datapoint = LwcDatapoint(1234567890L, "i-12345", tags, 42.0)

  private def toJson(v: LwcDatapoint): String = {
    val w = new StringWriter
    val gen = Json.newJsonGenerator(w)
    Json.encode(gen, v)
    gen.close()
    w.toString
  }

  @Benchmark
  def defaultEncode(bh: Blackhole): Unit = {
    bh.consume(toJson(datapoint))
  }

  @Benchmark
  def customEncode(bh: Blackhole): Unit = {
    bh.consume(datapoint.toJson)
  }
}
