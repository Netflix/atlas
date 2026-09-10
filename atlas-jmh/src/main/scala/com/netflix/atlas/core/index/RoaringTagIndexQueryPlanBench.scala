/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.core.index

import com.netflix.atlas.core.model.Query
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Benchmark for the query shapes that dominate the allocation profile of a data node.
  * See [[QueryPlanBenchData]] for how the index is shaped and why.
  *
  * An allocation profile of production showed 63.7% of all bytes coming from
  * `LazyOrBitmap.naivelazyor` (8k `long[1024]` bitmap containers) and 21% from
  * `RoaringBitmap.and`. The hot stacks were a left-deep `AND` chain whose innermost-left
  * leaf is an `:in`, so the union of the `:in` was fully materialized before anything
  * narrowed it.
  *
  * ```
  * > jmh:run -wi 5 -i 10 -f1 -t1 -prof gc \
  *     -jvmArgs "-Xmx8g -Xms8g" .*RoaringTagIndexQueryPlanBench.*
  * ```
  *
  * Watch `gc.alloc.rate.norm` (bytes/op) alongside throughput: the point of the change is
  * to cut allocation without giving up query time.
  */
@State(Scope.Thread)
class RoaringTagIndexQueryPlanBench {

  import QueryPlanBenchData.*

  private def run(bh: Blackhole, q: Query): Unit = {
    bh.consume(index.findItems(TagQuery(Some(q), limit = Integer.MAX_VALUE)))
  }

  @Benchmark
  def inLeftShallowQuery(bh: Blackhole): Unit = run(bh, inLeftShallow)

  @Benchmark
  def inLeftDeepQuery(bh: Blackhole): Unit = run(bh, inLeftDeep)

  @Benchmark
  def inLeftMixedQuery(bh: Blackhole): Unit = run(bh, inLeftMixed)

  @Benchmark
  def inLeftFiveQuery(bh: Blackhole): Unit = run(bh, inLeftFive)

  @Benchmark
  def inRightQuery(bh: Blackhole): Unit = run(bh, inRight)

  @Benchmark
  def patternLeftQuery(bh: Blackhole): Unit = run(bh, patternLeft)

  @Benchmark
  def rangeLeftQuery(bh: Blackhole): Unit = run(bh, rangeLeft)

  /** Guards the item-scan threshold: `acc` stays large, so distributing must win. */
  @Benchmark
  def inLargeAccQuery(bh: Blackhole): Unit = run(bh, inLargeAcc)

  /** Control: no other conjunct, so the planner has nothing to work with. */
  @Benchmark
  def inAloneQuery(bh: Blackhole): Unit = run(bh, inAlone)
}
