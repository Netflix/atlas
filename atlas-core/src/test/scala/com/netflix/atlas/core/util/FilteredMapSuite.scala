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

class FilteredMapSuite extends FunSuite {

  // Assert the view behaves identically to the materialized reference across the parts of the
  // Map contract that matter for tag maps, including the derived series id.
  private def assertSameAsMap(
    view: Map[String, String],
    reference: Map[String, String],
    probeKeys: Set[String]
  ): Unit = {
    assertEquals(view.size, reference.size, "size")
    assertEquals(view.isEmpty, reference.isEmpty, "isEmpty")

    assertEquals[Any, Any](view, reference, "view == reference")
    assertEquals[Any, Any](reference, view, "reference == view")
    assertEquals(view.hashCode, reference.hashCode, "hashCode")

    assertEquals(view.iterator.toMap, reference, "iterator")
    val collected = Map.newBuilder[String, String]
    view.foreachEntry((k, v) => collected += k -> v)
    assertEquals(collected.result(), reference, "foreachEntry")
    assertEquals(view.keySet, reference.keySet, "keySet")

    probeKeys.foreach { k =>
      assertEquals(view.get(k), reference.get(k), s"get($k)")
      assertEquals(view.getOrElse(k, "DFLT"), reference.getOrElse(k, "DFLT"), s"getOrElse($k)")
      assertEquals(view.contains(k), reference.contains(k), s"contains($k)")
    }

    assertEquals(
      TaggedItem.computeId(view),
      TaggedItem.computeId(reference),
      "computeId"
    )
  }

  private def check(underlying: Map[String, String], p: String => Boolean): Unit = {
    val probes = underlying.keySet ++ Set("absent", "name", "x")
    assertSameAsMap(new FilteredMap(underlying, p), underlying.filter(e => p(e._1)), probes)
  }

  private val tags = Map(
    "nf.app"     -> "www",
    "nf.cluster" -> "www-dev",
    "nf.node"    -> "i-123",
    "name"       -> "http.requests",
    "statistic"  -> "count"
  )

  test("subset of keys") {
    val keys = Set("name", "nf.app", "nf.cluster")
    check(tags, keys.contains)
  }

  test("predicate matches nothing") {
    check(tags, _ => false)
  }

  test("predicate matches everything") {
    check(tags, _ => true)
  }

  test("prefix predicate") {
    check(tags, _.startsWith("nf."))
  }

  test("get and getOrElse respect the predicate for present-but-filtered keys") {
    val view = new FilteredMap(tags, _ == "name")
    assertEquals(view.get("nf.app"), None)
    assertEquals(view.getOrElse("nf.app", "DFLT"), "DFLT")
    assert(!view.contains("nf.app"))
    assertEquals(view.get("name"), Some("http.requests"))
  }

  test("empty underlying") {
    check(Map.empty, _ => true)
  }

  test("updated and removed materialize correctly") {
    val view = new FilteredMap(tags, _.startsWith("nf."))
    assertEquals(view.updated("name", "x").get("name"), Some("x"))
    assertEquals(view.removed("nf.app").contains("nf.app"), false)
    assertEquals(view.removed("nf.app").contains("nf.cluster"), true)
  }
}
