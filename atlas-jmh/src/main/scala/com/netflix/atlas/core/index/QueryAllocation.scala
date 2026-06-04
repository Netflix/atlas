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

import com.netflix.atlas.core.model.BasicTaggedItem
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.util.SortedTagMap
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import java.util.UUID

/**
  * Allocation cost of query evaluation. Each `:eq`/`:hasKey` leaf used to clone the
  * matching bitmap from the index so an enclosing and/or could mutate it; a multi-term
  * query cloned once per term. With shared read-only leaves only the mutated
  * accumulator is copied (one clone for a left-nested chain). Compare
  * `gc.alloc.rate.norm` on `main` vs the change:
  *
  * ```
  * > jmh:run -wi 5 -i 8 -f1 -t1 -prof gc .*QueryAllocation.*
  * ```
  *
  * Design notes so the benchmark actually measures the clones:
  *  - Each key has many values and items get a random id, so after the id-sort the
  *    matching positions are scattered -> real ArrayContainer/BitmapContainer bitmaps
  *    (not run-compressed), which is the shape that dominated the alloc profile.
  *  - Queries are LEFT-nested (the shape ASL builds: `a,:eq,b,:eq,:and,c,:eq,:and`).
  *  - `limit = 1` suppresses result-list materialization so the bitmap-eval allocation
  *    is what is measured.
  */
@State(Scope.Thread)
class QueryAllocation {

  private val numItems = 20000
  private val numKeys = 5

  // Key k$j is "hi" when bit j of i is set, else "lo". So `k$j=hi` matches ~50% of
  // items, and because each item gets a random id the matches are scattered after the
  // id-sort -> BitmapContainer/ArrayContainer leaves (not run-compressed). The bits
  // are independent, so the left-nested AND stays non-empty through all 5 terms
  // (~1/32 of items) rather than short-circuiting to empty, and the OR covers ~31/32.
  private val items = (0 until numItems).map { i =>
    val tags = (0 until numKeys).map(j => s"k$j" -> (if (((i >> j) & 1) == 1) "hi" else "lo")).toMap
    BasicTaggedItem(SortedTagMap(tags + ("id" -> UUID.randomUUID().toString)))
  }

  private val index = RoaringTagIndex[BasicTaggedItem](items.toArray, new IndexStats())

  private def eq(j: Int): Query = Query.Equal(s"k$j", "hi")

  private val singleEq: Query = eq(0)

  // Left-nested: And(And(And(And(k0,k1),k2),k3),k4)
  private val and5: Query =
    (1 until numKeys).foldLeft(eq(0))((acc, j) => Query.And(acc, eq(j)))

  private val or5: Query =
    (1 until numKeys).foldLeft(eq(0))((acc, j) => Query.Or(acc, eq(j)))

  // limit = 1 so result materialization does not dominate the measurement.
  private def run(q: Query): List[BasicTaggedItem] =
    index.findItems(TagQuery(Some(q), limit = 1))

  @Benchmark
  def singleEquality(bh: Blackhole): Unit = bh.consume(run(singleEq))

  @Benchmark
  def andFiveTerms(bh: Blackhole): Unit = bh.consume(run(and5))

  @Benchmark
  def orFiveTerms(bh: Blackhole): Unit = bh.consume(run(or5))
}
