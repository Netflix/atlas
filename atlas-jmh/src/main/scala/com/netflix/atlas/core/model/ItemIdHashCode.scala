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
  * > jmh:run -prof gc -prof stack -wi 5 -i 10 -f1 -t1 .*ItemIdHashCode.*
  * ```
  *
  * ItemId.hashCode:
  *
  * ```
  * Benchmark              Mode  Cnt           Score          Error   Units
  * before                thrpt   10  1547433250.866 ± 52995197.574   ops/s
  * after                 thrpt   10  1162791300.824 ± 36880550.752   ops/s
  * ```
  *
  *
  * ItemId.intValue:
  *
  * ```
  * before                thrpt   10   292282926.019 ±  2643333.767   ops/s
  * after                 thrpt   10  1101391907.134 ± 14583585.092   ops/s
  * ```
  */
@State(Scope.Thread)
class ItemIdHashCode {

  private val id = ItemId(Random.nextBytes(20))

  @Benchmark
  def intValue(bh: Blackhole): Unit = {
    bh.consume(id.intValue)
  }

  @Benchmark
  def hashCode(bh: Blackhole): Unit = {
    bh.consume(id.hashCode())
  }
}
