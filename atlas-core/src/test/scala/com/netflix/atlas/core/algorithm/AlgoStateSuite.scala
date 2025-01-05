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
package com.netflix.atlas.core.algorithm

import com.netflix.atlas.json.Json
import munit.FunSuite

class AlgoStateSuite extends FunSuite {

  private def serde(state: AlgoState): AlgoState = {
    Json.decode[AlgoState](Json.encode(state))
  }

  private def check(state: AlgoState, get: AlgoState => Any, expected: Any): Unit = {
    // Map array to Seq
    // https://github.com/scalameta/munit/issues/339
    def f(v: Any): Any = v match {
      case a: Array[?] => a.toSeq
      case _           => v
    }
    assertEquals(f(get(state)), f(expected))
    assertEquals(f(get(serde(state))), f(expected))
  }

  test("boolean") {
    val s = AlgoState("foo", "test" -> true)
    check(s, _.getBoolean("test"), true)
  }

  test("int") {
    val s = AlgoState("foo", "test" -> 42)
    check(s, _.getInt("test"), 42)
  }

  test("long") {
    val s = AlgoState("foo", "test" -> 42L)
    check(s, _.getLong("test"), 42L)
  }

  test("long as double") {
    val s = AlgoState("foo", "test" -> 42L)
    check(s, _.getDouble("test"), 42.0)
  }

  test("double") {
    val s = AlgoState("foo", "test" -> 42.0)
    check(s, _.getDouble("test"), 42.0)
  }

  test("double: NaN") {
    val s = AlgoState("foo", "test" -> Double.NaN)
    assert(s.getDouble("test").isNaN)
    assert(serde(s).getDouble("test").isNaN)
  }

  test("double: invalid string") {
    val s = AlgoState("foo", "test" -> "invalid_numeric_value")
    intercept[NumberFormatException] {
      s.getDouble("test")
    }
  }

  test("double: invalid type") {
    val s = AlgoState("foo", "test" -> new Object)
    val e = intercept[IllegalStateException] {
      s.getDouble("test")
    }
    assertEquals(e.getMessage, "test has non-numeric type: class java.lang.Object")
  }

  test("double array") {
    val s = AlgoState("foo", "test" -> Array(42.0, 1.0))
    check(s, _.getDoubleArray("test"), Array(42.0, 1.0))
  }

  test("double array: NaN") {
    val s = AlgoState("foo", "test" -> Array(42.0, Double.NaN))
    val vs = s.getDoubleArray("test")
    assertEquals(vs(0), 42.0)
    assert(vs(1).isNaN)
    assertEquals(vs.count(!_.isNaN), 1)
  }

  test("double array: NaN serde") {
    val s = serde(AlgoState("foo", "test" -> Array(42.0, Double.NaN)))
    val vs = s.getDoubleArray("test")
    assertEquals(vs(0), 42.0)
    assert(vs(1).isNaN)
    assertEquals(vs.count(!_.isNaN), 1)
  }

  test("string") {
    val s = AlgoState("foo", "test" -> "42.0")
    check(s, _.getString("test"), "42.0")
  }

  test("sub-state") {
    val expected = AlgoState("bar", "test" -> 42)
    val s = AlgoState("foo", "test" -> expected)
    check(s, _.getState("test"), expected)
  }

  test("sub-state list") {
    val expected = (0 until 10).toList.map { i =>
      AlgoState("bar", "test" -> i)
    }
    val s = AlgoState("foo", "test" -> expected)
    check(s, _.getStateList("test"), expected)
  }
}
