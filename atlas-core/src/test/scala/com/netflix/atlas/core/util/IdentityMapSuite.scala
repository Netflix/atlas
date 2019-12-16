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

class IdentityMapSuite extends AnyFunSuite {

  test("add") {
    val m = IdentityMap.empty[String, Int] + ("a" -> 42)
    assert(m.get("a") === Some(42))
  }

  test("remove") {
    val m = IdentityMap(Map("a" -> 42)) - "a"
    assert(m.get("a") === None)
  }

  test("overwrite") {
    val m = IdentityMap(Map("a" -> 42)) + ("a" -> 2)
    assert(m.get("a") === Some(2))
  }

  test("iterate") {
    val m = IdentityMap(new String("a") -> 2, new String("a") -> 1, "b" -> 3)
    val values = m.iterator.map(_._2).toSet
    assert(values === Set(1, 2, 3))
  }

  test("toString") {
    val m = IdentityMap(new String("a") -> 2, new String("a") -> 1, "b" -> 3)

    // order is not deterministic
    val s = m.toString
    assert(s.contains("(a,1)"))
    assert(s.contains("(a,2)"))
    assert(s.contains("(b,3)"))
  }

  test("uses reference equality") {
    val m = IdentityMap.empty[String, Int] + ("a" -> 42)
    assert(m.get(new String("a")) === None)
  }

  test("++ preserves map type") {
    val m1 = IdentityMap(new String("a") -> 1)
    val m2 = IdentityMap(new String("a") -> 2)
    val m3 = m1 ++ m2
    assert(m3.size === 2)
    assert(m3.values.toSet === Set(1, 2))
  }
}
