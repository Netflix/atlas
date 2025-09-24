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
package com.netflix.atlas.core.util

import java.math.BigInteger

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Check use of `String.format` against just padding with our custom
  * `Strings.zeroPad`.
  *
  * ```
  * > jmh:run -prof gc -wi 10 -i 10 -f1 -t1 .*ZeroPad.*
  * ```
  *
  * Throughput
  *
  * ```
  * Benchmark                         Mode  Cnt        Score        Error   Units
  * hashArrayPad                     thrpt   10  4721308.898 ± 151781.009   ops/s
  * hashBigIntPad                    thrpt   10   867143.537 ±  19475.920   ops/s
  * hashFormat                       thrpt   10   519479.340 ±  36074.274   ops/s
  * oneArrayPad                      thrpt   10  7666430.180 ± 727283.845   ops/s
  * oneBigIntPad                     thrpt   10  3212304.227 ± 235176.496   ops/s
  * oneFormat                        thrpt   10   964090.075 ±  39087.882   ops/s
  * ```
  *
  * Allocations
  *
  * ```
  * hashArrayPad        gc.alloc.rate.norm   10      240.000 ±      0.001    B/op
  * hashBigIntPad       gc.alloc.rate.norm   10     1824.000 ±      0.001    B/op
  * hashFormat          gc.alloc.rate.norm   10     3344.000 ±      0.001    B/op
  * oneArrayPad         gc.alloc.rate.norm   10      240.000 ±      0.001    B/op
  * oneBigIntPad        gc.alloc.rate.norm   10      664.000 ±      0.001    B/op
  * oneFormat           gc.alloc.rate.norm   10     2176.000 ±      0.001    B/op
  * ```
  */
@State(Scope.Thread)
class ZeroPad {

  private val one = Array(1.toByte)
  private val hash = Hash.sha1("zero-pad")
  private val hashBytes = Hash.sha1bytes("zero-pad")

  @Benchmark
  def oneFormat(bh: Blackhole): Unit = {

    // Needs a lot of padding
    bh.consume("%040x".format(BigInteger.ONE))
  }

  @Benchmark
  def oneBigIntPad(bh: Blackhole): Unit = {

    // Needs a lot of padding
    bh.consume(Strings.zeroPad(BigInteger.ONE, 40))
  }

  @Benchmark
  def oneArrayPad(bh: Blackhole): Unit = {

    // Needs a lot of padding
    bh.consume(Strings.zeroPad(one, 40))
  }

  @Benchmark
  def hashFormat(bh: Blackhole): Unit = {

    // Common case, hash value that will likely not need much if any padding
    bh.consume("%040x".format(hash))
  }

  @Benchmark
  def hashBigIntPad(bh: Blackhole): Unit = {

    // Common case, hash value that will likely not need much if any padding
    bh.consume(Strings.zeroPad(hash, 40))
  }

  @Benchmark
  def hashArrayPad(bh: Blackhole): Unit = {

    // Common case, hash value that will likely not need much if any padding
    bh.consume(Strings.zeroPad(hashBytes, 40))
  }
}
