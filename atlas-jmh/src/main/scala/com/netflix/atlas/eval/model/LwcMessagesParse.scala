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

import org.apache.pekko.util.ByteString
import com.netflix.atlas.json.Json
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*LwcMessagesParse.*
  * ```
  *
  * Throughput:
  *
  * ```
  * Benchmark                          Mode  Cnt        Score        Error   Units
  * parseDatapoint                    thrpt   10  1138229.827 ±  24986.495   ops/s
  * parseDatapointBatch               thrpt   10  2151771.057 ±  54618.924   ops/s
  * parseDatapointByteString          thrpt   10  1334170.515 ±   9543.687   ops/s
  * parseDatapointByteStringUTF8      thrpt   10  1026735.084 ±   6455.626   ops/s
  * ```
  *
  * Allocations:
  *
  * ```
  * Benchmark                          Mode  Cnt        Score        Error   Units
  * parseDatapoint                    alloc   10     1680.000 ±      0.001    B/op
  * parseDatapointBatch               alloc   10     1528.000 ±      0.001    B/op
  * parseDatapointByteString          alloc   10     1976.000 ±      0.001    B/op
  * parseDatapointByteStringUTF8      alloc   10     2176.000 ±      0.001    B/op
  * ```
  **/
@State(Scope.Thread)
class LwcMessagesParse {

  private val tags = Map(
    "nf.app"     -> "atlas_backend",
    "nf.cluster" -> "atlas_backend-dev",
    "nf.node"    -> "i-123456789",
    "name"       -> "jvm.gc.pause",
    "statistic"  -> "totalTime"
  )

  private val datapoint = LwcDatapoint(1234567890L, "i-12345", tags, 42.0)
  private val json = Json.encode(datapoint)
  private val bytes = ByteString(json)
  private val batchBytes = LwcMessages.encodeBatch(Seq(datapoint))

  @Benchmark
  def parseDatapoint(bh: Blackhole): Unit = {
    bh.consume(LwcMessages.parse(json))
  }

  @Benchmark
  def parseDatapointByteString(bh: Blackhole): Unit = {
    bh.consume(LwcMessages.parse(bytes))
  }

  @Benchmark
  def parseDatapointByteStringUTF8(bh: Blackhole): Unit = {
    bh.consume(LwcMessages.parse(bytes.utf8String))
  }

  @Benchmark
  def parseDatapointBatch(bh: Blackhole): Unit = {
    bh.consume(LwcMessages.parseBatch(batchBytes))
  }
}
