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
package com.netflix.atlas.core.db

import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.CollectorStats
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.Query
import munit.FunSuite

class AggregateCollectorSuite extends FunSuite {

  private def newBuffer(v: Double, start: Long = 0L) = {
    new TimeSeriesBuffer(Map.empty, new ArrayTimeSeq(DsType.Gauge, start, 60000, Array.fill(1)(v)))
  }

  private def newTaggedBuffer(tags: Map[String, String], v: Double, start: Long = 0L) = {
    new TimeSeriesBuffer(tags, new ArrayTimeSeq(DsType.Gauge, start, 60000, Array.fill(1)(v)))
  }

  test("sum collector") {
    val c = new SumAggregateCollector
    assertEquals(c.result, Nil)
    c.add(newBuffer(1.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(1.0))
    assertEquals(c.result, List(newBuffer(4.0)))
    assertEquals(c.stats, CollectorStats(4, 4, 1, 1))
  }

  test("sum collector -- combine") {
    val c1 = new SumAggregateCollector
    c1.add(newBuffer(1.0))
    val c2 = new SumAggregateCollector
    c2.add(newBuffer(2.0))
    c1.combine(c2)
    assertEquals(c1.result, List(newBuffer(3.0)))
    assertEquals(c1.stats, CollectorStats(2, 2, 1, 1))
  }

  test("sum collector -- combine(null, c)") {
    val c1 = new SumAggregateCollector
    val c2 = new SumAggregateCollector
    c2.add(newBuffer(2.0))
    c1.combine(c2)
    assertEquals(c1.result, List(newBuffer(2.0)))
    assertEquals(c1.stats, CollectorStats(1, 1, 1, 1))
  }

  test("sum collector -- combine(c, null)") {
    val c1 = new SumAggregateCollector
    c1.add(newBuffer(1.0))
    val c2 = new SumAggregateCollector
    c1.combine(c2)
    assertEquals(c1.result, List(newBuffer(1.0)))
    assertEquals(c1.stats, CollectorStats(1, 1, 1, 1))
  }

  test("sum collector -- combine(null, c4)") {
    val c1 = new SumAggregateCollector
    val c2 = new SumAggregateCollector
    c2.add(newBuffer(1.0))
    c2.add(newBuffer(1.0))
    c2.add(newBuffer(1.0))
    c2.add(newBuffer(1.0))
    c1.combine(c2)
    assertEquals(c1.result, List(newBuffer(4.0)))
    assertEquals(c1.stats, CollectorStats(4, 4, 1, 1))
  }

  test("sum collector -- combine(c4, null)") {
    val c1 = new SumAggregateCollector
    c1.add(newBuffer(1.0))
    c1.add(newBuffer(1.0))
    c1.add(newBuffer(1.0))
    c1.add(newBuffer(1.0))
    val c2 = new SumAggregateCollector
    c1.combine(c2)
    assertEquals(c1.result, List(newBuffer(4.0)))
    assertEquals(c1.stats, CollectorStats(4, 4, 1, 1))
  }

  test("min collector") {
    val c = new MinAggregateCollector
    assertEquals(c.result, Nil)
    c.add(newBuffer(3.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(2.0))
    c.add(newBuffer(5.0))
    assertEquals(c.result, List(newBuffer(1.0)))
    assertEquals(c.stats, CollectorStats(4, 4, 1, 1))
  }

  test("max collector") {
    val c = new MaxAggregateCollector
    assertEquals(c.result, Nil)
    c.add(newBuffer(3.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(2.0))
    c.add(newBuffer(5.0))
    assertEquals(c.result, List(newBuffer(5.0)))
    assertEquals(c.stats, CollectorStats(4, 4, 1, 1))
  }

  test("all collector") {
    val expected = List(newBuffer(3.0), newBuffer(1.0), newBuffer(2.0), newBuffer(5.0))
    val c = new AllAggregateCollector
    assertEquals(c.result, Nil)
    expected.foreach(c.add)
    assertEquals(c.result, expected)
    assertEquals(c.stats, CollectorStats(4, 4, 4, 4))
  }

  test("by collector -- all") {
    val expected = List(
      newTaggedBuffer(Map("a" -> "1", "b" -> "2"), 3.0),
      newTaggedBuffer(Map("a" -> "2", "b" -> "2"), 1.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0),
      newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0)
    )
    val by = DataExpr.GroupBy(DataExpr.Sum(Query.False), List("a"))
    val c = new GroupByAggregateCollector(by)
    assertEquals(c.result, Nil)
    expected.foreach(c.add)
    assertEquals(c.result.toSet, expected.toSet)
    assertEquals(c.stats, CollectorStats(4, 4, 4, 4))
  }

  test("by collector -- grp") {
    val input = List(
      newTaggedBuffer(Map("a" -> "1", "b" -> "2"), 3.0),
      newTaggedBuffer(Map("a" -> "2", "b" -> "2"), 1.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0),
      newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0)
    )
    val expected = List(
      newTaggedBuffer(Map("a" -> "1", "b" -> "2"), 9.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0)
    )
    val by = DataExpr.GroupBy(DataExpr.Sum(Query.False), List("b"))
    val c = new GroupByAggregateCollector(by)
    assertEquals(c.result, Nil)
    input.foreach(c.add)
    assertEquals(c.result.toSet, expected.toSet)
    assertEquals(c.stats, CollectorStats(4, 4, 2, 2))
  }

  test("by collector -- missing key") {
    val input = List(
      newTaggedBuffer(Map("a" -> "1", "b" -> "2"), 3.0),
      newTaggedBuffer(Map("a" -> "2", "b" -> "2"), 1.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0),
      newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0)
    )
    val expected = List(newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0))
    val by = DataExpr.GroupBy(DataExpr.Sum(Query.False), List("b", "c"))
    val c = new GroupByAggregateCollector(by)
    assertEquals(c.result, Nil)
    input.foreach(c.add)
    assertEquals(c.result.toSet, expected.toSet)
    assertEquals(c.stats, CollectorStats(1, 1, 1, 1))
  }
}
