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
package com.netflix.atlas.eval.stream

import java.nio.charset.StandardCharsets

import org.apache.pekko.util.ByteString
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.json.Json
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Compare decoding a ByteString to JSON:
  *
  * 1. ByteString > String > Json.decode
  * 2. ByteString > Array[Byte] (reused) > Json.decode
  *
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*ByteStringDecode.*
  * ```
  *
  * Initial results:
  *
  * ```
  * Throughput:
  *
  * Benchmark                   Mode  Cnt       Score       Error   Units
  * decodeFromByteString       thrpt   10  344793.104 ± 11891.506   ops/s
  * decodeFromString           thrpt   10  299394.816 ±  9240.783   ops/s
  *
  *
  * Allocation rate per operation:
  *
  * Benchmark                   Mode  Cnt       Score       Error   Units
  * decodeFromByteString       thrpt   10    3928.000 ±     0.002    B/op
  * decodeFromString           thrpt   10    4680.001 ±     0.002    B/op
  * ```
  */
@State(Scope.Thread)
class ByteStringDecode {

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
  private val datapointJson = Json.encode(datapoint)
  private val datapointBytes = ByteString(datapointJson)

  private val buffer = new Array[Byte](16384)

  @Benchmark
  def decodeFromString(bh: Blackhole): Unit = {
    val str = datapointBytes.decodeString(StandardCharsets.UTF_8)
    val obj = Json.decode[LwcDatapoint](str)
    bh.consume(obj)
  }

  @Benchmark
  def decodeFromByteString(bh: Blackhole): Unit = {
    val length = datapointBytes.length
    datapointBytes.copyToArray(buffer, 0, length)
    val obj = Json.decode[LwcDatapoint](buffer, 0, length)
    bh.consume(obj)
  }
}
