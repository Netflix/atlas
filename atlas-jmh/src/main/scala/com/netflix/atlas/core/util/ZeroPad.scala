/*
 * Copyright 2014-2016 Netflix, Inc.
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
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Check use of `String.format` against just padding with our custom
  * `Strings.zeroPad`.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*ZeroPad.*
  * ```
  *
  * Initial results:
  *
  * ```
  * [info] Benchmark                Mode  Cnt        Score        Error  Units
  * [info] ZeroPad.hashFormat      thrpt   10   494319.963 ±  28396.222  ops/s
  * [info] ZeroPad.hashPad         thrpt   10   794528.844 ±  47922.416  ops/s
  * [info] ZeroPad.oneFormat       thrpt   10   904410.430 ±  55786.780  ops/s
  * [info] ZeroPad.onePad          thrpt   10  3391001.622 ± 288190.044  ops/s
  * ```
  */
@State(Scope.Thread)
class ZeroPad {

  private val hash = Hash.sha1("zero-pad")

  @Threads(1)
  @Benchmark
  def oneFormat(bh: Blackhole): Unit = {
    // Needs a lot of padding
    bh.consume("%040x".format(BigInteger.ONE))
  }

  @Threads(1)
  @Benchmark
  def onePad(bh: Blackhole): Unit = {
    // Needs a lot of padding
    bh.consume(Strings.zeroPad(BigInteger.ONE, 40))
  }

  @Threads(1)
  @Benchmark
  def hashFormat(bh: Blackhole): Unit = {
    // Common case, hash value that will likely not need much if any padding
    bh.consume("%040x".format(hash))
  }

  @Threads(1)
  @Benchmark
  def hashPad(bh: Blackhole): Unit = {
    // Common case, hash value that will likely not need much if any padding
    bh.consume(Strings.zeroPad(hash, 40))
  }
}
