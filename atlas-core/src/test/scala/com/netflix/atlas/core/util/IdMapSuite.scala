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

import com.netflix.spectator.api.Id
import org.scalatest.funsuite.AnyFunSuite

class IdMapSuite extends AnyFunSuite {

  test("basic map") {
    assert(IdMap(Id.create("foo")) === Map("name" -> "foo"))
  }

  test("removed") {
    val id = Id.create("foo").withTags("a", "1", "b", "2")
    assert(IdMap(id).removed("name") === Map("a" -> "1", "b"   -> "2"))
    assert(IdMap(id).removed("a") === Map("name" -> "foo", "b" -> "2"))
  }

  test("updated") {
    val id = Id.create("foo").withTags("a", "1")
    assert(IdMap(id).updated("b", "2") === Map("name" -> "foo", "a" -> "1", "b" -> "2"))
  }

  test("get") {
    val id = Id.create("foo").withTags("a", "1", "b", "2")
    assert(IdMap(id).get("name") === Some("foo"))
    assert(IdMap(id).get("a") === Some("1"))
    assert(IdMap(id).get("b") === Some("2"))
    assert(IdMap(id).get("c") === None)
  }

  test("iterator") {
    val id = Id.create("foo").withTags("a", "1", "b", "2")
    val expected = List(
      "name" -> "foo",
      "a"    -> "1",
      "b"    -> "2"
    )
    assert(IdMap(id).iterator.toList === expected)
  }

  test("foreachEntry") {
    val id = Id.create("foo").withTags("a", "1", "b", "2")
    val builder = List.newBuilder[(String, String)]
    IdMap(id).foreachEntry { (k, v) =>
      builder += k -> v
    }
    val actual = builder.result()
    val expected = List(
      "name" -> "foo",
      "a"    -> "1",
      "b"    -> "2"
    )
    assert(actual === expected)
  }
}
