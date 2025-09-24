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

import com.netflix.atlas.core.model.LazyTaggedItem
import com.netflix.spectator.api.NoopRegistry
import munit.FunSuite

class BatchUpdateTagIndexSuite extends FunSuite {

  case class Item(tags: Map[String, String], version: Int) extends LazyTaggedItem

  private def newIndex: BatchUpdateTagIndex[Item] = {
    BatchUpdateTagIndex.newRoaringIndex(new NoopRegistry)
  }

  test("update") {
    val idx = newIndex
    assertEquals(idx.size, 0)

    val updates = List(Item(Map("a" -> "b"), 0))
    idx.update(updates)
    assertEquals(idx.size, 0)

    idx.rebuildIndex()
    assertEquals(idx.findItems(TagQuery(None)), updates)
  }

  test("update, new items") {
    val idx = newIndex
    assertEquals(idx.size, 0)

    val updates1 = List(Item(Map("a" -> "b"), 0))
    idx.update(updates1)
    idx.rebuildIndex()
    assertEquals(idx.findItems(TagQuery(None)), updates1)

    val updates2 = List(Item(Map("b" -> "c"), 0))
    idx.update(updates2)
    idx.rebuildIndex()

    val expected = (updates1 ::: updates2).sortWith { (a, b) =>
      a.id.compareTo(b.id) < 0
    }
    assertEquals(idx.findItems(TagQuery(None)), expected)
  }

  test("update, prefer older item") {
    val idx = newIndex
    assertEquals(idx.size, 0)

    val updates1 = List(Item(Map("a" -> "b"), 0))
    idx.update(updates1)
    idx.rebuildIndex()
    assertEquals(idx.findItems(TagQuery(None)), updates1)

    val updates2 = List(Item(Map("a" -> "b"), 1))
    idx.update(updates2)
    idx.rebuildIndex()
    assertEquals(idx.findItems(TagQuery(None)), updates1)
  }
}
