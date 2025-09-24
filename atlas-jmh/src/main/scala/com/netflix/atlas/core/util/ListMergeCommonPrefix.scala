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
  * Sanity check performance of utility for merging sorted lists.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*ListMergeCommonPrefix.*
  * ...
  * Benchmark                         Mode  Cnt   Score   Error  Units
  * ListMergeCommonPrefix.merge      thrpt   10   8.700 ± 0.160  ops/s
  * ```
  */
@State(Scope.Thread)
class ListMergeCommonPrefix {

  private val prefix = "abcdefghi" * 20

  private val vs = (0 until 2000).map { _ =>
    (0 until 1000).map { i =>
      f"$prefix-$i%05d"
    }.toList
  }.toList

  @Benchmark
  def merge(bh: Blackhole): Unit = {
    bh.consume(ListHelper.merge(1000, vs))
  }

}
