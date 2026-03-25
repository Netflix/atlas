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
package com.netflix.atlas.lsp

import munit.FunSuite

class GlossarySuite extends FunSuite {

  test("load: parses sample glossary") {
    val g = Glossary.load("sample-glossary.json")
    assertEquals(g.id, "nflx.sample")
    assert(g.metrics.nonEmpty)
    assert(g.tagKeys.nonEmpty)
    assert(g.tagValues.nonEmpty)
    assert(g.metrics.contains("sys.cpu.utilization"))
    assert(g.tagKeys.contains("statistic"))
    assert(g.tagValues.contains("statistic=count"))
  }

  test("load: metric fields parsed correctly") {
    val g = Glossary.load("sample-glossary.json")
    val cpu = g.metrics("sys.cpu.utilization")
    assertEquals(cpu.unit, Some("percent"))
    assertEquals(cpu.category, Some("system/cpu"))
    assertEquals(cpu.`type`, Some("gauge"))
    assert(cpu.tags.contains("id"))
    val idTag = cpu.tags("id")
    assertEquals(idTag.valuesType, Some("examples"))
    assert(idTag.values.get.contains("user"))
  }

  test("load: tag key with values and valuesType") {
    val g = Glossary.load("sample-glossary.json")
    val stat = g.tagKeys("statistic")
    assertEquals(stat.valuesType, Some("enum"))
    assert(stat.values.get.contains("count"))
    assert(stat.values.get.contains("totalTime"))
  }

  test("load: empty glossary") {
    val g = Glossary.empty
    assertEquals(g.id, "empty")
    assert(g.metrics.isEmpty)
    assert(g.tagKeys.isEmpty)
    assert(g.tagValues.isEmpty)
  }

  test("merge: combines metrics") {
    val a = Glossary(
      id = "a",
      metrics = Map(
        "m1" -> MetricDef(description = "metric one")
      )
    )
    val b = Glossary(
      id = "b",
      metrics = Map(
        "m2" -> MetricDef(description = "metric two")
      )
    )
    val merged = Glossary.merge(a, b)
    assertEquals(merged.id, "b")
    assert(merged.metrics.contains("m1"))
    assert(merged.metrics.contains("m2"))
  }

  test("merge: later description wins for same metric") {
    val a = Glossary(
      id = "a",
      metrics = Map(
        "m1" -> MetricDef(description = "old", unit = Some("bytes"))
      )
    )
    val b = Glossary(
      id = "b",
      metrics = Map(
        "m1" -> MetricDef(description = "new")
      )
    )
    val merged = Glossary.merge(a, b)
    assertEquals(merged.metrics("m1").description, "new")
    assertEquals(merged.metrics("m1").unit, Some("bytes"))
  }

  test("merge: valuesType examples wins") {
    val a = Glossary(
      id = "a",
      tagKeys = Map(
        "k" -> TagKeyDef(
          description = "key",
          values = Some(List("a")),
          valuesType = Some("enum")
        )
      )
    )
    val b = Glossary(
      id = "b",
      tagKeys = Map(
        "k" -> TagKeyDef(
          description = "key",
          values = Some(List("b")),
          valuesType = Some("examples")
        )
      )
    )
    val merged = Glossary.merge(a, b)
    assertEquals(merged.tagKeys("k").valuesType, Some("examples"))
  }

  test("merge: values are unioned") {
    val a = Glossary(
      id = "a",
      tagKeys = Map(
        "k" -> TagKeyDef(description = "key", values = Some(List("a", "b")))
      )
    )
    val b = Glossary(
      id = "b",
      tagKeys = Map(
        "k" -> TagKeyDef(description = "key", values = Some(List("b", "c")))
      )
    )
    val merged = Glossary.merge(a, b)
    val values = merged.tagKeys("k").values.get
    assertEquals(values.sorted, List("a", "b", "c"))
  }

  test("merge: tag values later wins") {
    val a = Glossary(
      id = "a",
      tagValues = Map("k=v" -> TagValueDef(description = "old"))
    )
    val b = Glossary(
      id = "b",
      tagValues = Map("k=v" -> TagValueDef(description = "new"))
    )
    val merged = Glossary.merge(a, b)
    assertEquals(merged.tagValues("k=v").description, "new")
  }

  test("load: missing resource throws") {
    intercept[IllegalArgumentException] {
      Glossary.load("nonexistent.json")
    }
  }
}
