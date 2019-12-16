/*
 * Copyright 2014-2019 Netflix, Inc.
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

import org.scalatest.funsuite.AnyFunSuite

class ArrayHelperSuite extends AnyFunSuite {

  test("merge arrays, limit 1: empty, one") {
    val v1 = Array.empty[String]
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assert(actual === Array("a"))
  }

  test("merge arrays, limit 1: empty, abcde") {
    val v1 = Array.empty[String]
    val v2 = Array("a", "b", "c", "d", "e")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assert(actual === Array("a"))
  }

  test("merge arrays, limit 1: abcde, empty") {
    val v1 = Array("a", "b", "c", "d", "e")
    val v2 = Array.empty[String]
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assert(actual === Array("a"))
  }

  test("merge arrays, limit 1: b, a") {
    val v1 = Array("b")
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assert(actual === Array("a"))
  }

  test("merge arrays, limit 1: a, b") {
    val v1 = Array("a")
    val v2 = Array("b")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toArray
    assert(actual === Array("a"))
  }

  test("merge arrays, limit 2: b, a") {
    val v1 = Array("b")
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toArray
    assert(actual === Array("a", "b"))
  }

  test("merge arrays, limit 2: ab, a") {
    val v1 = Array("a", "b")
    val v2 = Array("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toArray
    assert(actual === Array("a", "b"))
  }

  test("merge arrays, limit 2: bc, ad") {
    val v1 = Array("b", "c")
    val v2 = Array("a", "d")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toArray
    assert(actual === Array("a", "b"))
  }

  test("merge list, limit 1: empty, one") {
    val v1 = List.empty[String]
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assert(actual === List("a"))
  }

  test("merge list, limit 1: empty, abcde") {
    val v1 = List.empty[String]
    val v2 = List("a", "b", "c", "d", "e")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assert(actual === List("a"))
  }

  test("merge list, limit 1: abcde, empty") {
    val v1 = List("a", "b", "c", "d", "e")
    val v2 = List.empty[String]
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assert(actual === List("a"))
  }

  test("merge list, limit 1: b, a") {
    val v1 = List("b")
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assert(actual === List("a"))
  }

  test("merge list, limit 1: a, b") {
    val v1 = List("a")
    val v2 = List("b")
    val actual = ArrayHelper.merger[String](1).merge(v1).merge(v2).toList
    assert(actual === List("a"))
  }

  test("merge list, limit 2: b, a") {
    val v1 = List("b")
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toList
    assert(actual === List("a", "b"))
  }

  test("merge list, limit 2: ab, a") {
    val v1 = List("a", "b")
    val v2 = List("a")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toList
    assert(actual === List("a", "b"))
  }

  test("merge list, limit 2: bc, ad") {
    val v1 = List("b", "c")
    val v2 = List("a", "d")
    val actual = ArrayHelper.merger[String](2).merge(v1).merge(v2).toList
    assert(actual === List("a", "b"))
  }
}
