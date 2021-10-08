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

class ListHelperSuite extends FunSuite {

  test("merge two sorted lists") {
    val v1 = List("a", "c", "d")
    val v2 = List("b", "e", "f")
    assertEquals(ListHelper.merge(10, v1, v2), List("a", "b", "c", "d", "e", "f"))
  }

  test("merge two sorted lists with limit") {
    val v1 = List("a", "c", "d")
    val v2 = List("b", "e")
    assertEquals(ListHelper.merge(2, v1, v2), List("a", "b"))
  }

  test("merge empty and sorted list") {
    val v1 = List.empty[String]
    val v2 = List("a", "b", "c")
    assertEquals(ListHelper.merge(2, v1, v2), List("a", "b"))
  }

  test("merge empty and sorted list with size equal to limit") {
    val v1 = List.empty[String]
    val v2 = List("a", "b")
    assertEquals(ListHelper.merge(2, v1, v2), List("a", "b"))
  }

  test("merge empty and sorted list with size less than limit") {
    val v1 = List.empty[String]
    val v2 = List("a")
    assertEquals(ListHelper.merge(2, v1, v2), List("a"))
  }

  test("merge sorted and empty list") {
    val v1 = List("a", "b", "c")
    val v2 = List.empty[String]
    assertEquals(ListHelper.merge(2, v1, v2), List("a", "b"))
  }

  test("merge many sorted lists with limit") {
    val v1 = List("a", "c", "d")
    val v2 = List("b", "e")
    val v3 = List("aa", "d")
    val v4 = List("z")
    assertEquals(ListHelper.merge(3, List(v1, v2, v3, v4)), List("a", "aa", "b"))
  }

  test("dedup while merging") {
    val v1 = List("a", "c", "d")
    val v2 = List("a", "b", "f")
    assertEquals(ListHelper.merge(10, v1, v2), List("a", "b", "c", "d", "f"))
  }
}
