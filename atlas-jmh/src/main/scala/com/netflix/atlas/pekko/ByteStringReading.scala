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
package com.netflix.atlas.pekko

import java.util.Random
import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Results:
  *
  * ```
  * Benchmark                        Mode  Cnt          Score          Error   Units
  * toArray                         thrpt   10          9.092 ±        0.540   ops/s
  * inputStream                     thrpt   10        159.626 ±       21.220   ops/s
  *
  * Benchmark                        Mode  Cnt          Score          Error   Units
  * toArray            gc.alloc.rate.norm   10  104857658.225 ±       64.153    B/op
  * inputStream        gc.alloc.rate.norm   10       4185.017 ±        3.531    B/op
  * ```
  */
@State(Scope.Benchmark)
class ByteStringReading {

  private val random = new Random()
  private val byteArray = new Array[Byte](1024 * 1024 * 100)
  random.nextBytes(byteArray)
  private val byteString = ByteString(byteArray)

  private val compositeByteString = {
    val builder = ByteString.newBuilder
    (0 until 1024).foreach { _ =>
      val bytes = new Array[Byte](1024 * 100)
      random.nextBytes(bytes)
      builder.append(ByteString(bytes))
    }
    builder.result()
  }

  @Benchmark
  def toArray(bh: Blackhole): Unit = {
    bh.consume(byteString.toArray)
  }

  @Benchmark
  def inputStream(bh: Blackhole): Unit = {
    val in = new ByteStringInputStream(byteString)
    val buffer = new Array[Byte](4096)
    var len = in.read(buffer)
    while (len > 0) {
      bh.consume(buffer)
      len = in.read(buffer)
    }
  }

  @Benchmark
  def compositeToArray(bh: Blackhole): Unit = {
    bh.consume(compositeByteString.toArray)
  }

  @Benchmark
  def compositeInputStream(bh: Blackhole): Unit = {
    val in = new ByteStringInputStream(compositeByteString)
    val buffer = new Array[Byte](4096)
    var len = in.read(buffer)
    while (len > 0) {
      bh.consume(buffer)
      len = in.read(buffer)
    }
  }
}
