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
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TimeSeries
import org.roaringbitmap.RoaringBitmap

class RoaringTagIndexSuite extends TagIndexSuite {

  val index: TagIndex[TimeSeries] = {
    RoaringTagIndex(TagIndexSuite.dataset.toArray, new IndexStats())
  }

  test("empty") {
    val idx = RoaringTagIndex.empty[Datapoint]
    assert(idx.size == 0)
  }

  test("hasNonEmptyIntersection: empty, empty") {
    val b1 = new RoaringBitmap()
    val b2 = new RoaringBitmap()
    assert(!RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }

  test("hasNonEmptyIntersection: equal") {
    val b1 = new RoaringBitmap()
    b1.add(10)
    val b2 = new RoaringBitmap()
    b2.add(10)
    assert(RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }

  test("hasNonEmptyIntersection: no match") {
    val b1 = new RoaringBitmap()
    (0 until 20 by 2).foreach(b1.add)
    val b2 = new RoaringBitmap()
    (1 until 21 by 2).foreach(b2.add)
    assert(!RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }

  test("hasNonEmptyIntersection: last match") {
    val b1 = new RoaringBitmap()
    (0 until 22 by 2).foreach(b1.add)
    val b2 = new RoaringBitmap()
    (1 until 21 by 2).foreach(b2.add)
    b2.add(20)
    assert(RoaringTagIndex.hasNonEmptyIntersection(b1, b2))
  }

  private def createLessThanIndex: TagIndex[BasicTaggedItem] = {
    val items = (0 until 10).map { i =>
      val tags = Map(
        "name" -> "test",
        "a"    -> i.toString,
        "b"    -> "foo"
      )
      BasicTaggedItem(tags)
    }
    RoaringTagIndex(items.toArray, new IndexStats())
  }

  test("lt, first element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThan("a", "0")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, Set.empty[String])
  }

  test("lt, second element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThan("a", "1")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, Set("0"))
  }

  test("lt, missing element after first for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThan("a", "00")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, Set("0"))
  }

  test("lt, last element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThan("a", "9")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, (0 until 9).map(_.toString).toSet)
  }

  test("lt, after last element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThan("a", "a")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, (0 until 10).map(_.toString).toSet)
  }

  test("le, first element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThanEqual("a", "0")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, Set("0"))
  }

  test("le, second element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThanEqual("a", "1")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, Set("0", "1"))
  }

  test("le, missing element after first for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThanEqual("a", "00")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, Set("0"))
  }

  test("le, last element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThanEqual("a", "9")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, (0 until 10).map(_.toString).toSet)
  }

  test("le, after last element for tag") {
    val idx = createLessThanIndex
    val q = Query.LessThanEqual("a", "a")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("a")).toSet, (0 until 10).map(_.toString).toSet)
  }

  private def createGreaterThanIndex: TagIndex[BasicTaggedItem] = {
    val items = (0 until 10).map { i =>
      val tags = Map(
        "name" -> "test",
        "z"    -> i.toString,
        "b"    -> "foo"
      )
      BasicTaggedItem(tags)
    }
    RoaringTagIndex(items.toArray, new IndexStats())
  }

  test("gt, first element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThan("z", "0")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, (1 until 10).map(_.toString).toSet)
  }

  test("gt, second element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThan("z", "1")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, (2 until 10).map(_.toString).toSet)
  }

  test("gt, missing element after first for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThan("z", "00")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, (1 until 10).map(_.toString).toSet)
  }

  test("gt, last element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThan("z", "9")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, Set.empty[String])
  }

  test("gt, after last element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThan("z", "a")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, Set.empty[String])
  }

  test("ge, first element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThanEqual("z", "0")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, (0 until 10).map(_.toString).toSet)
  }

  test("ge, second element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThanEqual("z", "1")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, (1 until 10).map(_.toString).toSet)
  }

  test("ge, missing element after first for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThanEqual("z", "00")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, (1 until 10).map(_.toString).toSet)
  }

  test("ge, last element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThanEqual("z", "9")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, Set("9"))
  }

  test("ge, after last element for tag") {
    val idx = createGreaterThanIndex
    val q = Query.GreaterThanEqual("z", "a")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("z")).toSet, Set.empty[String])
  }

  // Tests for lessThan/lessThanEqual when the computed value position in the global
  // values array doesn't correspond to an existing tag for the queried key. This
  // causes tagOffset to return an insertion point rather than an exact match, and
  // the backward iteration can incorrectly include the first element after the
  // insertion point.
  test("lt, value from other key between queried key values") {
    // "Zvalue" from key "name" sits between nothing and "a1javasandbox" in the global
    // values array. When querying k < "a", findOffsetLessThan maps to the position of
    // "Zvalue", which is not a value for key "k", so the tag doesn't exist in tagIndex.
    val items = Array(
      BasicTaggedItem(Map("name" -> "Zvalue", "k" -> "a1javasandbox")),
      BasicTaggedItem(Map("name" -> "Zvalue", "k" -> "Zasg"))
    )
    val idx = RoaringTagIndex(items, new IndexStats())
    val q = Query.LessThan("k", "a")
    val rs = idx.findItems(TagQuery(Some(q)))
    // "Zasg" < "a" is true (uppercase Z < lowercase a), "a1javasandbox" < "a" is false
    assertEquals(rs.map(_.tags("k")).toSet, Set("Zasg"))
  }

  test("lt, no values less than target") {
    val items = Array(
      BasicTaggedItem(Map("name" -> "Zvalue", "k" -> "a1javasandbox"))
    )
    val idx = RoaringTagIndex(items, new IndexStats())
    val q = Query.LessThan("k", "a")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("k")).toSet, Set.empty[String])
  }

  test("le, value from other key between queried key values") {
    val items = Array(
      BasicTaggedItem(Map("name" -> "Zvalue", "k" -> "a1javasandbox")),
      BasicTaggedItem(Map("name" -> "Zvalue", "k" -> "Zasg"))
    )
    val idx = RoaringTagIndex(items, new IndexStats())
    val q = Query.LessThanEqual("k", "a")
    val rs = idx.findItems(TagQuery(Some(q)))
    assertEquals(rs.map(_.tags("k")).toSet, Set("Zasg"))
  }

  // --------------------------------------------------------------------------
  // Ownership / shared-bitmap safety.
  //
  // findReadOnly may hand back a reference to a bitmap stored in the index (for
  // Equal/HasKey/True). If any query path mutated one of those, the index would be
  // silently corrupted and later queries for the same tag would return wrong
  // results. These tests run leaf queries, then a battery of compound queries that
  // route those leaves through the in-place and/or, and assert the leaf results are
  // unchanged afterward.
  // --------------------------------------------------------------------------

  // Each test builds its own index: a buggy implementation would corrupt the shared
  // one and the tests would corrupt each other (order-dependent failures). A fresh
  // index per test makes a regression fail deterministically.
  private def freshIndex(): TagIndex[TimeSeries] =
    RoaringTagIndex(TagIndexSuite.dataset.toArray, new IndexStats())

  private def ids(idx: TagIndex[TimeSeries], q: Query): List[com.netflix.atlas.core.model.ItemId] =
    idx.findItems(TagQuery(Some(q))).map(_.id)

  // Real (key, value) pairs and keys drawn from the dataset so the queries match.
  private val sampleTags: List[(String, String)] =
    TagIndexSuite.dataset.iterator.flatMap(_.tags).toList.distinct.take(12)

  private val sampleKeys: List[String] = sampleTags.map(_._1).distinct

  // The key with the most distinct values, and its (sorted) values. Used to build
  // In/Regex/range leaves so the ownership battery also exercises the non-Equal
  // leaves as and/or operands -- locking the invariant that they return fresh sets.
  private val multiKey: String =
    TagIndexSuite.dataset.iterator
      .flatMap(_.tags)
      .toList
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2).distinct.size)
      .maxBy(_._2)
      ._1

  private val multiVals: List[String] =
    TagIndexSuite.dataset.iterator
      .flatMap(_.tags)
      .collect { case (k, v) if k == multiKey => v }
      .toList
      .distinct
      .sorted

  private val inQuery: Query = Query.In(multiKey, multiVals.take(3))
  private val regexQuery: Query = Query.Regex(multiKey, multiVals.head.take(2))
  private val geQuery: Query = Query.GreaterThanEqual(multiKey, multiVals(multiVals.size / 2))

  private val leafQueries: List[Query] =
    Query.True :: inQuery :: regexQuery :: geQuery ::
      sampleKeys.map(Query.HasKey.apply) :::
      sampleTags.map { case (k, v) => Query.Equal(k, v) }

  test("ownership: AND does not corrupt the shared bitmap of its first operand") {
    val idx = freshIndex()
    val q = Query.Equal(sampleTags.head._1, sampleTags.head._2)
    val before = ids(idx, q)
    // q is the first operand, evaluated via findOwned (must clone); the in-place
    // `and` then mutates the accumulator. AND with *different* equality tags so the
    // intersection is a strict subset -- if the clone is missing, q's stored bitmap
    // shrinks. Guard against a vacuous setup: require at least one term to shrink q.
    assert(
      sampleTags.tail.exists {
        case (k, v) => ids(idx, Query.And(q, Query.Equal(k, v))).size < before.size
      },
      "test setup is vacuous: no AND term shrinks the first operand"
    )
    sampleTags.tail.foreach {
      case (k, v) => idx.findItems(TagQuery(Some(Query.And(q, Query.Equal(k, v)))))
    }
    assertEquals(ids(idx, q), before, "Equal result changed after AND -> shared bitmap mutated")
  }

  test("ownership: OR does not corrupt the shared bitmap of its first operand") {
    val idx = freshIndex()
    val q = Query.Equal(sampleTags.head._1, sampleTags.head._2)
    val before = ids(idx, q)
    // Guard against vacuity: require at least one OR term to grow the result.
    assert(
      sampleTags.tail.exists {
        case (k, v) => ids(idx, Query.Or(q, Query.Equal(k, v))).size > before.size
      },
      "test setup is vacuous: no OR term grows the first operand"
    )
    // in-place `or` would add bits to q's stored bitmap if it were not cloned.
    sampleTags.tail.foreach {
      case (k, v) => idx.findItems(TagQuery(Some(Query.Or(q, Query.Equal(k, v)))))
    }
    assertEquals(ids(idx, q), before, "Equal result changed after OR -> shared bitmap mutated")
  }

  test("ownership: HasKey/True survive NOT and a deep compound battery") {
    val idx = freshIndex()
    val baseline = leafQueries.map(q => q -> ids(idx, q)).toMap

    val a = Query.Equal(sampleTags.head._1, sampleTags.head._2)
    val b = Query.Equal(sampleTags(1)._1, sampleTags(1)._2)
    val compounds = List(
      Query.And(a, Query.HasKey(sampleKeys.head)),
      Query.And(Query.HasKey(sampleKeys.head), a),
      Query.Or(a, b),
      Query.Or(Query.True, a),
      Query.Not(a),
      Query.And(Query.Not(a), Query.HasKey(sampleKeys.head)),
      Query.And(Query.And(a, Query.HasKey(sampleKeys.head)), b),
      Query.Or(Query.Or(a, b), Query.HasKey(sampleKeys.last)),
      // Non-Equal leaves as the first (mutated) operand -- locks the invariant that
      // In/Regex/range return fresh sets and don't need a copy in findOwned.
      Query.And(inQuery, Query.HasKey(sampleKeys.head)),
      Query.Or(regexQuery, a),
      Query.And(geQuery, Query.HasKey(sampleKeys.head))
    )
    // Run twice to surface progressive corruption (each run would further
    // shrink/grow a shared bitmap that was wrongly aliased).
    (0 until 2).foreach(_ => compounds.foreach(c => idx.findItems(TagQuery(Some(c)))))

    leafQueries.foreach { q =>
      assertEquals(ids(idx, q), baseline(q), s"leaf query changed after compound battery: $q")
    }
  }

  test("ownership: offset query does not corrupt `all`") {
    val idx = freshIndex()
    val full = idx.findItems(TagQuery(None, limit = Int.MaxValue))
    val before = full.map(_.id)
    // Use an id from the MIDDLE of the sorted set so itemOffset(someId) > 0 and the
    // offset>0 branch (which clones `all` before removing the prefix) actually runs.
    // The lowest id is at sorted position 0 and would yield offset 0 (read-only path).
    val someId = full(full.size / 2).id.toString
    val paged = idx.findItems(TagQuery(None, offset = someId, limit = Int.MaxValue))
    assert(paged.size < full.size, "offset did not trim; offset>0 clone path not exercised")
    // Run both offset>0 paths: no-query (all.clone()) and True-query (findOwned(True)).
    idx.findItems(TagQuery(Some(Query.True), offset = someId, limit = Int.MaxValue))
    idx.findItems(TagQuery(None, offset = someId, limit = Int.MaxValue))
    assertEquals(
      idx.findItems(TagQuery(None, limit = Int.MaxValue)).map(_.id),
      before,
      "all changed after offset query -> shared `all` was mutated"
    )
  }

  test("offset: paging a :not query excludes ids at/below the cursor") {
    // Locks the offset-deferral fix: previously offset was applied per-leaf and the
    // Not branch (`andNot(all, ...)`) left `all` untrimmed, so paging a :not query
    // re-emitted the already-paged prefix. Offset is now applied once on the result.
    val idx = freshIndex()
    val q = Query.Not(Query.Equal(sampleTags.head._1, sampleTags.head._2))
    val full = idx.findItems(TagQuery(Some(q), limit = Int.MaxValue))
    val cursor = full(full.size / 2).id.toString
    val paged = idx.findItems(TagQuery(Some(q), offset = cursor, limit = Int.MaxValue))
    assert(paged.nonEmpty && paged.size < full.size, "offset did not page the :not result")
    assert(
      paged.forall(_.id.toString > cursor),
      "paged :not result contains ids <= the offset cursor"
    )
  }
}
