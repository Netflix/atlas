/*
 * Copyright 2014-2020 Netflix, Inc.
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

import akka.util.ByteString
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
  * parseDatapoint                    thrpt    5  1228277.874 ± 43946.724   ops/s
  * parseDatapointByteString          thrpt    5  1381682.708 ±  74560.401   ops/s
  * parseDatapointByteStringUTF8      thrpt    5  1019061.212 ±  40733.781   ops/s
  * ```
  *
  * Allocations:
  *
  * ```
  * Benchmark                          Mode  Cnt        Score        Error   Units
  * parseDatapoint                    alloc    5     1632.000 ±      0.001    B/op
  * parseDatapointByteString          alloc    5     1816.000 ±      0.001    B/op
  * parseDatapointByteStringUTF8      alloc    5     2128.000 ±      0.001    B/op
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
}
