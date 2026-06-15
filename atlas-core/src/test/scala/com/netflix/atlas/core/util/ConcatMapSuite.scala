/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.atlas.core.model.TaggedItem
import munit.FunSuite

class ConcatMapSuite extends FunSuite {

  // Assert the view behaves identically to the materialized reference across the parts of the
  // Map contract that matter for tag maps, including the derived series id.
  private def assertSameAsMap(
    view: Map[String, String],
    reference: Map[String, String],
    probeKeys: Set[String]
  ): Unit = {
    assertEquals(view.size, reference.size, "size")
    assertEquals(view.isEmpty, reference.isEmpty, "isEmpty")

    // equals in both directions and a consistent hashCode
    assertEquals[Any, Any](view, reference, "view == reference")
    assertEquals[Any, Any](reference, view, "reference == view")
    assertEquals(view.hashCode, reference.hashCode, "hashCode")

    // entries via iterator and foreachEntry
    assertEquals(view.iterator.toMap, reference, "iterator")
    val collected = Map.newBuilder[String, String]
    view.foreachEntry((k, v) => collected += k -> v)
    assertEquals(collected.result(), reference, "foreachEntry")
    assertEquals(view.keySet, reference.keySet, "keySet")

    // per-key lookups for present and absent keys
    probeKeys.foreach { k =>
      assertEquals(view.get(k), reference.get(k), s"get($k)")
      assertEquals(view.getOrElse(k, "DFLT"), reference.getOrElse(k, "DFLT"), s"getOrElse($k)")
      assertEquals(view.contains(k), reference.contains(k), s"contains($k)")
    }

    // series id must be stable vs the materialized map
    assertEquals(
      TaggedItem.computeId(view),
      TaggedItem.computeId(reference),
      "computeId"
    )
  }

  private def check(a: Map[String, String], b: Map[String, String]): Unit = {
    val probes = a.keySet ++ b.keySet ++ Set("absent", "name", "x")
    assertSameAsMap(new ConcatMap(a, b), a ++ b, probes)
  }

  test("disjoint keys") {
    check(Map("a" -> "1", "b" -> "2"), Map("c" -> "3", "d" -> "4"))
  }

  test("overlapping keys: b takes precedence") {
    val view = new ConcatMap(Map("a" -> "1", "b" -> "1"), Map("b" -> "2", "c" -> "3"))
    assertEquals(view.get("b"), Some("2"))
    check(Map("a" -> "1", "b" -> "1"), Map("b" -> "2", "c" -> "3"))
  }

  test("empty left") {
    check(Map.empty, Map("a" -> "1", "b" -> "2"))
  }

  test("empty right") {
    check(Map("a" -> "1", "b" -> "2"), Map.empty)
  }

  test("both empty") {
    val view = new ConcatMap(Map.empty, Map.empty)
    assert(view.isEmpty)
    assertEquals(TaggedItem.computeId(view), TaggedItem.computeId(Map.empty[String, String]))
  }

  test("realistic tag maps") {
    val common = Map("nf.app" -> "www", "nf.cluster" -> "www-common", "nf.region" -> "us-east-1")
    val series = Map("name" -> "http.requests", "nf.node" -> "i-123", "nf.cluster" -> "www-dev")
    check(common, series)
  }

  test("updated and removed materialize correctly") {
    val view = new ConcatMap(Map("a" -> "1"), Map("b" -> "2"))
    assertEquals(view.updated("c", "3"), Map("a" -> "1", "b" -> "2", "c" -> "3"))
    assertEquals(view.removed("a"), Map("b" -> "2"))
  }

  test("null value in rhs is treated as present (equals stays symmetric)") {
    val a = Map("k" -> "fromA")
    val b = Map("k" -> (null: String))
    val view = new ConcatMap(a, b)
    val reference = a ++ b
    // b wins on the conflict, even when its value is null
    assertEquals(view.get("k"), reference.get("k"))
    assertEquals(view.getOrElse("k", "DFLT"), reference.getOrElse("k", "DFLT"))
    // inherited Map.equals probes getOrElse, so this must hold in both directions
    assertEquals[Any, Any](view, reference)
    assertEquals[Any, Any](reference, view)
  }
}
