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

import munit.FunSuite

import java.util.UUID

class ArrayHelperSuite extends FunSuite {

  test("merge arrays, limit 1: empty, one") {
    val v1 = Array.empty[String]
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a").toSeq)
  }

  test("merge arrays, limit 1: empty, abcde") {
    val v1 = Array.empty[String]
    val v2 = Array("a", "b", "c", "d", "e")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a").toSeq)
  }

  test("merge arrays, limit 1: abcde, empty") {
    val v1 = Array("a", "b", "c", "d", "e")
    val v2 = Array.empty[String]
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a").toSeq)
  }

  test("merge arrays, limit 1: b, a") {
    val v1 = Array("b")
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a").toSeq)
  }

  test("merge arrays, limit 1: a, b") {
    val v1 = Array("a")
    val v2 = Array("b")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a").toSeq)
  }

  test("merge arrays, limit 2: b, a") {
    val v1 = Array("b")
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a", "b").toSeq)
  }

  test("merge arrays, limit 2: ab, a") {
    val v1 = Array("a", "b")
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a", "b").toSeq)
  }

  test("merge arrays, limit 2: aggregate duplicates") {
    type T = (String, Int)
    val v1 = Array("a" -> 1, "b" -> 1)
    val v2 = Array("a" -> 1)
    val actual = ArrayHelper
      .merger[T](2, (a: T, b: T) => a._1.compareTo(b._1), (a: T, b: T) => a._1 -> (a._2 + b._2))
      .merge(v1)
      .merge(v2)
      .toArray
    assertEquals(actual.toSeq, Array("a" -> 2, "b" -> 1).toSeq)
  }

  test("merge arrays, limit 2: bc, ad") {
    val v1 = Array("b", "c")
    val v2 = Array("a", "d")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toArray
    assertEquals(actual.toSeq, Array("a", "b").toSeq)
  }

  test("merge: both fully consumed") {
    val comparator = new ComparableComparator[String]
    val pickFirst = (v: String, _: String) => v
    val v1 = Array("b", "c")
    val v2 = Array("a", "d")
    val merged = Array("", "")
    val length = ArrayHelper.merge(comparator, pickFirst, v1, 1, v2, 1, merged)
    assertEquals(length, 2)
    assertEquals(merged.toSeq, Seq("a", "b"))
    assert(v1(0) == null)
    assert(v2(0) == null)
  }

  test("merge: v1 partially consumed") {
    val comparator = new ComparableComparator[String]
    val pickFirst = (v: String, _: String) => v
    val v1 = Array("b", "c")
    val v2 = Array("a", "d")
    val merged = Array("", "")
    val length = ArrayHelper.merge(comparator, pickFirst, v1, 2, v2, 1, merged)
    assertEquals(length, 2)
    assertEquals(merged.toSeq, Seq("a", "b"))
    assert(v1(0) != null)
    assert(v2(0) == null)
  }

  test("merge: v2 partially consumed") {
    val comparator = new ComparableComparator[String]
    val pickFirst = (v: String, _: String) => v
    val v1 = Array("b", "c")
    val v2 = Array("a", "d")
    val merged = Array("", "")
    val length = ArrayHelper.merge(comparator, pickFirst, v1, 1, v2, 2, merged)
    assertEquals(length, 2)
    assertEquals(merged.toSeq, Seq("a", "b"))
    assert(v1(0) == null)
    assert(v2(0) != null)
  }

  test("merge: both partially consumed") {
    val comparator = new ComparableComparator[String]
    val pickFirst = (v: String, _: String) => v
    val v1 = Array("b", "c")
    val v2 = Array("a", "d")
    val merged = Array("", "")
    val length = ArrayHelper.merge(comparator, pickFirst, v1, 2, v2, 2, merged)
    assertEquals(length, 2)
    assertEquals(merged.toSeq, Seq("a", "b"))
    assert(v1(0) != null)
    assert(v2(0) != null)
  }

  test("merge list, limit 1: empty, one") {
    val v1 = List.empty[String]
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assertEquals(actual, List("a"))
  }

  test("merge list, limit 1: empty, abcde") {
    val v1 = List.empty[String]
    val v2 = List("a", "b", "c", "d", "e")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assertEquals(actual, List("a"))
  }

  test("merge list, limit 1: abcde, empty") {
    val v1 = List("a", "b", "c", "d", "e")
    val v2 = List.empty[String]
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assertEquals(actual, List("a"))
  }

  test("merge list, limit 1: b, a") {
    val v1 = List("b")
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assertEquals(actual, List("a"))
  }

  test("merge list, limit 1: a, b") {
    val v1 = List("a")
    val v2 = List("b")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assertEquals(actual, List("a"))
  }

  test("merge list, limit 2: b, a") {
    val v1 = List("b")
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toList
    assertEquals(actual, List("a", "b"))
  }

  test("merge list, limit 2: ab, a") {
    val v1 = List("a", "b")
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toList
    assertEquals(actual, List("a", "b"))
  }

  test("merge list, limit 2: bc, ad") {
    val v1 = List("b", "c")
    val v2 = List("a", "d")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toList
    assertEquals(actual, List("a", "b"))
  }

  test("sortAndDedup, empty") {
    val vs = Array.empty[String]
    val length = ArrayHelper.sortAndDedup(vs)
    assertEquals(length, 0)
  }

  test("sortAndDedup, one item") {
    val vs = Array("a")
    val length = ArrayHelper.sortAndDedup(vs)
    assertEquals(length, 1)
  }

  test("sortAndDedup, one item after dedup") {
    val vs = Array("a", "a", "a")
    val length = ArrayHelper.sortAndDedup(vs)
    assertEquals(length, 1)
  }

  test("sortAndDedup, several items") {
    val vs = Array("c", "a", "b", "a")
    val length = ArrayHelper.sortAndDedup(vs)
    assertEquals(length, 3)
    assertEquals(vs.toSeq.take(length), Seq("a", "b", "c"))
  }

  test("sortAndDedup, several items aggregate") {
    type T = (String, Int)
    val vs = Array("c", "a", "b", "b", "a", "d", "a").map(k => k -> 1)
    val length = ArrayHelper.sortAndDedup(
      (a: T, b: T) => a._1.compareTo(b._1),
      (a: T, b: T) => a._1 -> (a._2 + b._2),
      vs,
      vs.length
    )
    assertEquals(length, 4)
    assertEquals(vs.toSeq.take(length), Seq("a" -> 3, "b" -> 2, "c" -> 1, "d" -> 1))
  }

  test("sortAndDedup, partially filled array") {
    type T = (String, Int)
    val vs = Array("c", "a", "b", "b", "a", "d", "a", null, null)
      .map(k => if (k == null) null else k -> 1)
    val length = ArrayHelper.sortAndDedup(
      (a: T, b: T) => a._1.compareTo(b._1),
      (a: T, b: T) => a._1 -> (a._2 + b._2),
      vs,
      7
    )
    assertEquals(length, 4)
    assertEquals(vs.toSeq.take(length), Seq("a" -> 3, "b" -> 2, "c" -> 1, "d" -> 1))
  }

  test("sortPairs, empty") {
    val data = Array.empty[String]
    ArrayHelper.sortPairs(data)
  }

  test("sortPairs, single pair") {
    val data = Array("b", "1")
    ArrayHelper.sortPairs(data)
    val expected = Array("b", "1")
    assertEquals(expected.toList, data.toList)
  }

  test("sortPairs, two pairs") {
    val data = Array("b", "1", "a", "2")
    ArrayHelper.sortPairs(data)
    val expected = Array("a", "2", "b", "1")
    assertEquals(expected.toList, data.toList)
  }

  test("sortPairs, random") {
    val input = (0 until 50).map(i => UUID.randomUUID().toString -> i.toString)
    val data = input.flatMap(t => List(t._1, t._2)).toArray
    ArrayHelper.sortPairs(data)
    val expected = input.toList.sortWith(_._1 < _._1).flatMap(t => List(t._1, t._2))
    assertEquals(expected, data.toList)
  }
}
