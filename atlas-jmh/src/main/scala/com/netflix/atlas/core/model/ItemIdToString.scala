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
package com.netflix.atlas.core.model

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import scala.util.Random

/**
  * ```
  * > jmh:run -prof gc -prof stack -wi 5 -i 10 -f1 -t1 .*ItemIdToString.*
  * ...
  * Benchmark                             Mode  Cnt        Score       Error   Units
  * runToString                          thrpt   10  6502790.539 ± 84203.781   ops/s
  * runToString:·gc.alloc.rate           thrpt   10     1275.402 ±    16.731  MB/sec
  * runToString:·gc.alloc.rate.norm      thrpt   10      216.000 ±     0.001    B/op
  * ```
  */
@State(Scope.Thread)
class ItemIdToString {

  private val id = ItemId(Random.nextBytes(20))

  @Benchmark
  def runToString(bh: Blackhole): Unit = {
    bh.consume(id.toString)
  }
}
