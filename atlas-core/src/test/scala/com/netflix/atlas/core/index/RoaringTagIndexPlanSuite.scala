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
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import scala.util.Random

/**
  * Differential test for the `AND` planner. Conjunctions are reordered and the
  * intersection is distributed over `In`/pattern terms, so the result must be checked
  * against a brute force scan rather than against a hand written expectation: the
  * interesting failures are the ones where a reordering changes what a term is evaluated
  * against.
  */
class RoaringTagIndexPlanSuite extends FunSuite {

  // The constructor requires the array to be sorted by id already -- only the `apply`
  // factory sorts it. These indexes are built with `new` so the subclass can pin the
  // strategy, so the sort has to happen here or id-offset paging breaks.
  private val items = {
    val arr = TagIndexSuite.dataset.toArray
    java.util.Arrays.sort(arr, RoaringTagIndex.IdComparator)
    arr
  }

  // The dataset is far smaller than the production threshold, so left to itself a
  // union-building term would always take the candidate-walk branch. Pin the threshold at
  // both extremes so each strategy is covered and the two are checked against each other.
  private class Index(threshold: Int) extends RoaringTagIndex[TimeSeries](items, new IndexStats()) {
    override protected def itemScanThreshold: Int = threshold
  }

  private val walkIndex = new Index(Int.MaxValue)
  private val unionIndex = new Index(0)
  private val index = new Index(8192)

  /**
    * Reference implementation: evaluate the query directly against every item's tags.
    * Ids rather than tag maps, and an ordered list rather than a set, so that a dropped
    * duplicate or a reordered result is visible -- `findItems` has to come back in id order
    * for the id-offset paging below to make progress, and reordering the chain must not
    * disturb that. `items` is sorted by id, so this is already in the expected order.
    */
  private def bruteForce(q: Query): List[String] = {
    items.filter(i => q.matches(i.tags)).map(_.idString).toList
  }

  private def resultOf(idx: TagIndex[TimeSeries], q: Query): List[String] = {
    idx.findItems(TagQuery(Some(q), limit = Integer.MAX_VALUE)).map(_.idString)
  }

  private def indexResult(q: Query): List[String] = resultOf(index, q)

  private def check(q: Query): Unit = {
    val expected = bruteForce(q)
    assertEquals(resultOf(walkIndex, q), expected, s"candidate-walk mismatch for: $q")
    assertEquals(resultOf(unionIndex, q), expected, s"union mismatch for: $q")
    // The production default as well, not just the two pinned extremes.
    assertEquals(resultOf(index, q), expected, s"default threshold mismatch for: $q")
  }

  // Keys and values actually present in the dataset, plus a few that are not so the
  // empty-set paths get exercised too.
  private val keys =
    List("nf.app", "nf.cluster", "nf.node", "nf.stack", "name", "type", "type2", "missing")

  // Memoized: this scans every item, and randomLeaf calls it on each draw.
  private val valuesByKey: Map[String, List[String]] = keys.map { k =>
    val present = items.flatMap(_.tags.get(k)).distinct.sorted.toList
    k -> (present :+ "nope")
  }.toMap

  private def valuesFor(k: String): List[String] = valuesByKey(k)

  private def randomLeaf(r: Random): Query = {
    val k = keys(r.nextInt(keys.length))
    val vs = valuesFor(k)
    val v = vs(r.nextInt(vs.size))
    r.nextInt(8) match {
      case 0 => Query.Equal(k, v)
      case 1 => Query.HasKey(k)
      case 2 => Query.In(k, r.shuffle(vs).take(1 + r.nextInt(4)))
      case 3 => Query.Regex(k, "^" + v.take(2))
      case 4 => Query.Not(Query.Equal(k, v))
      case 5 => Query.GreaterThan(k, v)
      case 6 => Query.LessThan(k, v)
      case _ => Query.Regex(k, v.take(3))
    }
  }

  private def randomQuery(r: Random): Query = {
    val n = 2 + r.nextInt(4)
    (0 until n).map(_ => randomLeaf(r)).reduce((a, b) => Query.And(a, b))
  }

  test("and chain matches brute force for random queries") {
    val r = new Random(42)
    (0 until 60).foreach(_ => check(randomQuery(r)))
  }

  test("and chain matches brute force with nested or") {
    val r = new Random(7)
    (0 until 30).foreach { _ =>
      val q = Query.And(
        Query.Or(randomLeaf(r), randomLeaf(r)),
        Query.And(randomLeaf(r), randomLeaf(r))
      )
      check(q)
    }
  }

  test("conjunction of only negations") {
    val r = new Random(11)
    (0 until 30).foreach { _ =>
      val q = Query.And(Query.Not(randomLeaf(r)), Query.Not(randomLeaf(r)))
      check(q)
    }
  }

  test("reordering does not change the result") {
    val r = new Random(99)
    (0 until 30).foreach { _ =>
      val leaves = (0 until (2 + r.nextInt(3))).map(_ => randomLeaf(r)).toList
      val a = leaves.reduce((x, y) => Query.And(x, y))
      val b = r.shuffle(leaves).reduce((x, y) => Query.And(x, y))
      assertEquals(indexResult(a), indexResult(b), s"order changed result: $a vs $b")
    }
  }

  test("an In term is not mutated by being intersected against") {
    // The index bitmaps are shared; neither strategy may write into them. Run the same In
    // twice and confirm the standalone result is stable.
    val vs = valuesFor("nf.cluster").take(3)
    val standalone = Query.In("nf.cluster", vs)
    val before = indexResult(standalone)
    check(Query.And(Query.Equal("nf.app", "nccp"), standalone))
    assertEquals(indexResult(standalone), before)
    assertEquals(indexResult(standalone), bruteForce(standalone))
  }

  test("range terms match brute force at the boundaries") {
    // The walk turns a range into a comparison of positions in the sorted values array, so the
    // interesting cases are the ends of that array and the exact boundary value.
    val app = Query.Equal("nf.app", "nccp")
    val vs = valuesFor("nf.cluster").filter(_ != "nope")
    val lowest = vs.head
    val highest = vs.last
    val middle = vs(vs.size / 2)

    // "!" sorts below every value in the dataset, "~" above every one.
    val below = "!"
    val above = "~"

    val bounds = List(below, lowest, middle, highest, above, "nope")
    bounds.foreach { b =>
      check(Query.And(app, Query.GreaterThan("nf.cluster", b)))
      check(Query.And(app, Query.GreaterThanEqual("nf.cluster", b)))
      check(Query.And(app, Query.LessThan("nf.cluster", b)))
      check(Query.And(app, Query.LessThanEqual("nf.cluster", b)))
      // A key that is not in the index at all.
      check(Query.And(app, Query.GreaterThan("missing", b)))
      check(Query.And(app, Query.LessThan("missing", b)))
    }

    // A bound that exists in the values array but only under a different key: the position is
    // resolved globally, so this has to fall outside the cluster range rather than inside it.
    val nameValue = valuesFor("name").head
    check(Query.And(app, Query.GreaterThan("nf.cluster", nameValue)))
    check(Query.And(app, Query.LessThan("nf.cluster", nameValue)))
  }

  test("offset paging still works with a reordered chain") {
    val q = Query.And(
      Query.In("nf.cluster", valuesFor("nf.cluster").take(3)),
      Query.HasKey("name")
    )
    val expected = index.findItems(TagQuery(Some(q), limit = Integer.MAX_VALUE))
    // Only items with a distinct id are reachable by id-offset paging: the offset is
    // resolved with a binary search over the sorted id array, so a repeated id cannot
    // advance past itself. The dataset is not guaranteed to be free of those.
    val reachable = expected.map(_.idString).distinct
    val pageSize = 10
    val seen = List.newBuilder[String]
    var page = index.findItems(TagQuery(Some(q), limit = pageSize))
    var iterations = 0
    val maxIterations = reachable.size + 10
    while (page.size == pageSize && iterations < maxIterations) {
      seen ++= page.map(_.idString)
      page = index.findItems(TagQuery(Some(q), offset = page.last.idString, limit = pageSize))
      iterations += 1
    }
    seen ++= page.map(_.idString)
    assert(iterations < maxIterations, s"paging did not terminate after $iterations pages")
    assertEquals(seen.result().distinct, reachable)
  }
}
