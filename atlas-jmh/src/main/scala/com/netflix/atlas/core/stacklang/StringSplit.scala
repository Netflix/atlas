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
package com.netflix.atlas.core.stacklang

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Simple test for splitting a stacklang expression by the `,`.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*StringSplit.*
  * ...
  * Benchmark                      Mode  Cnt       Score       Error  Units
  * StringSplit.naive             thrpt   10  207502.367 ±  5054.394  ops/s
  * StringSplit.splitAndTrim      thrpt   10  554580.700 ± 27627.109  ops/s
  * ```
  */
@State(Scope.Thread)
class StringSplit {

  private val value = (0 until 50).mkString(",")

  @Benchmark
  def naive(bh: Blackhole): Unit = {
    bh.consume(value.trim.split("\\s*,\\s*").filter(_.nonEmpty).toList)
  }

  @Benchmark
  def splitAndTrim(bh: Blackhole): Unit = {
    bh.consume(Interpreter.splitAndTrim(value))
  }

}
