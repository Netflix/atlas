/*
 * Copyright 2014-2021 Netflix, Inc.
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
