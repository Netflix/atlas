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

/**
  * Synthetic index shaped to reproduce the container mix seen in a production allocation
  * profile of a data node.
  *
  * Roaring stores a 64k block as an `ArrayContainer` up to 4096 entries and promotes it to
  * a `BitmapContainer` (a `long[1024]`, 8k) above that. With 1M items the index spans 16
  * blocks, so a tag value matching more than ~65k items has bitmap-container leaves and one
  * matching fewer has array-container leaves. Both are needed: the profile's three
  * `long[]` sites are `appendCopy`/`clone` (seeding the accumulator from a dense leaf),
  * `lazyIOR`/`toBitmapContainer` and `mergeBulk`/`toBitmapContainer` (promoting a sparse
  * leaf while merging).
  *
  * Tag values are assigned from independent hash streams so the dimensions are
  * uncorrelated; deriving them all from `i % k` makes the conjunctions collapse to empty.
  */
object QueryPlanBenchData {

  val numItems = 1_000_000

  private val numApps = 100 // ~10,000 items each (1%) -- the selective term
  private val numRegions = 4 // ~250,000 items each
  private val numDenseNames = 4 // ~125,000 items each -- BitmapContainer leaves
  private val numSparseNames = 16 // ~31,250 items each  -- ArrayContainer leaves

  private val statistics = Array("count", "totalTime", "max")

  /** Independent pseudo-random stream per tag dimension. */
  private def h(i: Int, salt: Int): Int = {
    var x = i * 0x9E3779B1 + salt * 0x85EBCA77
    x ^= x >>> 15
    x *= 0x2545F491
    x ^= x >>> 13
    x & 0x7FFFFFFF
  }

  private def nameFor(i: Int): String = {
    if (h(i, 3) % 2 == 0) f"name_${h(i, 4) % numDenseNames}%02d"
    else f"name_${numDenseNames + (h(i, 5) % numSparseNames)}%02d"
  }

  val items: Array[BasicTaggedItem] = {
    val arr = (0 until numItems).map { i =>
      val app = f"app_${h(i, 1) % numApps}%03d"
      val region = s"region_${h(i, 2) % numRegions}"
      BasicTaggedItem(
        SortedTagMap(
          Map(
            "nf.app"     -> app,
            "nf.cluster" -> s"$app-main",
            "nf.stack"   -> "main",
            "nf.region"  -> region,
            "nf.zone"    -> s"$region${('a' + (h(i, 6) % 3)).toChar}",
            "nf.node"    -> f"i-$i%09d",
            "name"       -> nameFor(i),
            "statistic"  -> statistics(h(i, 7) % 3)
          )
        )
      )
    }.toArray
    // Every index below uses the constructor directly, which requires the array to be
    // sorted by id already. Sorting once here rather than letting `apply` do it again.
    java.util.Arrays.sort(arr, RoaringTagIndex.IdComparator)
    arr
  }

  lazy val index: RoaringTagIndex[BasicTaggedItem] =
    new RoaringTagIndex[BasicTaggedItem](items, new IndexStats())

  /** Index with the intersection strategy pinned, to measure whether the item-scan branch
    * earns its keep against plain per-value distribution. */
  val app: Query = Query.Equal("nf.app", "app_042")
  val region: Query = Query.Equal("nf.region", "region_1")
  val statistic: Query = Query.Equal("statistic", "count")
  val notMax: Query = Query.Not(Query.Equal("statistic", "max"))

  // 58% of `:in` in the production corpus have <= 2 values, 97% have <= 5.
  val inDense: Query = Query.In("name", List("name_00", "name_01"))
  val inMixed: Query = Query.In("name", List("name_00", "name_05", "name_11"))

  val inFive: Query =
    Query.In("name", List("name_00", "name_01", "name_02", "name_07", "name_13"))
  val namePattern: Query = Query.Regex("name", "^name_0")

  // Matches name_00..name_09: four dense values plus six sparse ones, so the union a range
  // builds spans both container types.
  val nameRange: Query = Query.LessThan("name", "name_1")

  /** The profile shape: `:in` is the innermost-left leaf, so its union is built first. */
  val inLeftShallow: Query = Query.And(Query.And(inDense, app), statistic)

  /** 4 conjuncts with a trailing negation; 24% of the corpus has this shape. */
  val inLeftDeep: Query = Query.And(Query.And(Query.And(inDense, app), region), notMax)

  /** Mixed container densities across the `:in` values. */
  val inLeftMixed: Query = Query.And(Query.And(inMixed, app), statistic)

  /** 5-value `:in`, the 97th percentile of the corpus. */
  val inLeftFive: Query = Query.And(Query.And(inFive, app), statistic)

  /** Same conjuncts with the `:in` already on the right: reordering is a no-op, only
    * distributing the AND over the union helps. */
  val inRight: Query = Query.And(Query.And(app, statistic), inDense)

  /** Pattern query instead of `:in`; 5.9% of the corpus. */
  val patternLeft: Query = Query.And(Query.And(namePattern, app), statistic)

  /** Range query, the widest union of the term types. */
  val rangeLeft: Query = Query.And(Query.And(nameRange, app), statistic)

  /** Only a broad conjunct to narrow with, so `acc` stays large (~250k). This is the case
    * the item-scan threshold has to protect: walking a quarter million items to check two
    * values is worse than two intersections. */
  val inLargeAcc: Query = Query.And(region, inDense)

  /** Control: nothing to distribute over, so nothing to gain. */
  val inAlone: Query = inDense

  val all: List[(String, Query)] = List(
    "inLeftShallow  And(And(In2,app),stat)"          -> inLeftShallow,
    "inLeftDeep     And(And(And(In2,app),reg),!max)" -> inLeftDeep,
    "inLeftMixed    And(And(In3,app),stat)"          -> inLeftMixed,
    "inLeftFive     And(And(In5,app),stat)"          -> inLeftFive,
    "inRight        And(And(app,stat),In2)"          -> inRight,
    "patternLeft    And(And(re,app),stat)"           -> patternLeft,
    "rangeLeft      And(And(lt,app),stat)"           -> rangeLeft,
    "inLargeAcc     And(region,In2)"                 -> inLargeAcc,
    "inAlone        In(name,[n0,n1])"                -> inAlone
  )
}
