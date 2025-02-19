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
}
