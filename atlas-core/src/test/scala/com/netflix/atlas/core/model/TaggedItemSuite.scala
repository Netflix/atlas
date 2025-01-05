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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

import java.util.UUID

class TaggedItemSuite extends FunSuite {

  def expectedId(tags: Map[String, String]): ItemId = {
    ItemId(
      Hash.sha1bytes(tags.toList.sortWith(_._1 < _._1).map(t => t._1 + "=" + t._2).mkString(","))
    )
  }

  test("computeId, name only") {
    val t1 = Map("name" -> "foo")
    val t2 = Map("name" -> "bar")
    assertEquals(TaggedItem.computeId(t1), expectedId(t1))
    assertEquals(TaggedItem.computeId(t2), expectedId(t2))
    assert(TaggedItem.computeId(t1) != TaggedItem.computeId(t2))
  }

  test("computeId, two") {
    val t1 = Map("name" -> "foo", "cluster" -> "abc")
    val t2 = Map("name" -> "bar", "cluster" -> "abc")
    assertEquals(TaggedItem.computeId(t1), expectedId(t1))
    assertEquals(TaggedItem.computeId(t2), expectedId(t2))
    assert(TaggedItem.computeId(t1) != TaggedItem.computeId(t2))
  }

  test("computeId, multi") {
    val t1 = Map("name" -> "foo", "cluster" -> "abc", "app" -> "a", "zone" -> "1")
    val t2 = Map("name" -> "foo", "cluster" -> "abc", "app" -> "a", "zone" -> "2")
    assertEquals(TaggedItem.computeId(t1), expectedId(t1))
    assertEquals(TaggedItem.computeId(t2), expectedId(t2))
    assert(TaggedItem.computeId(t1) != TaggedItem.computeId(t2))
  }

  test("computeId, sorted tag map") {
    val t1 = SortedTagMap("name" -> "foo", "cluster" -> "abc", "app" -> "a", "zone" -> "1")
    val t2 = SortedTagMap("name" -> "foo", "cluster" -> "abc", "app" -> "a", "zone" -> "2")
    assertEquals(TaggedItem.computeId(t1), expectedId(t1))
    assertEquals(TaggedItem.computeId(t2), expectedId(t2))
    assert(TaggedItem.computeId(t1) != TaggedItem.computeId(t2))
  }

  test("computeId, large tag maps") {
    // verify buffers grow as expected
    (10 until 10_000 by 1000).foreach { size =>
      val tags = (0 until size).map(i => i.toString -> UUID.randomUUID().toString).toMap
      val smallTags = SortedTagMap(tags)
      assertEquals(TaggedItem.computeId(smallTags), expectedId(tags))
    }
  }
}
