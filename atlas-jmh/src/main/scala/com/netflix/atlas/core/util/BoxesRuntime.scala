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

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * ```
  * Benchmark                         Mode  Cnt          Score         Error  Units
  * BoxesRuntime.javaEquals          thrpt   10  112495954.095 ± 2795842.625  ops/s
  * BoxesRuntime.scalaEquals         thrpt   10   82963668.047 ± 6225820.287  ops/s
  * ```
  */
@State(Scope.Thread)
class BoxesRuntime {

  private val a = "a896b12f-1863-4568-98b2-1f3b1aee55cA"
  private val b = "a896b12f-1863-4568-98b2-1f3b1aee55cB"

  @Benchmark
  def javaEquals(bh: Blackhole): Unit = {
    bh.consume(a != null && a.equals(b))
  }

  @Benchmark
  def scalaEquals(bh: Blackhole): Unit = {
    // Compiles to BoxesRunTime.equals(a, b)
    bh.consume(a == b)
  }
}
