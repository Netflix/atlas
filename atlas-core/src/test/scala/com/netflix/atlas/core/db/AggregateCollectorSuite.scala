/*
 * Copyright 2015 Netflix, Inc.
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
import org.scalatest.FunSuite

class AggregateCollectorSuite extends FunSuite {

  import java.lang.{Double => JDouble}

  private def newBuffer(v: Double, start: Long = 0L) = {
    new TimeSeriesBuffer(Map.empty, new ArrayTimeSeq(DsType.Gauge, start, 60000, Array.fill(1)(v)))
  }

  private def newTaggedBuffer(tags: Map[String, String], v: Double, start: Long = 0L) = {
    new TimeSeriesBuffer(tags, new ArrayTimeSeq(DsType.Gauge, start, 60000, Array.fill(1)(v)))
  }

  test("sum collector") {
    val c = new SumAggregateCollector
    assert(c.result === Nil)
    c.add(newBuffer(1.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(1.0))
    assert(c.result === List(newBuffer(4.0)))
    assert(c.stats === CollectorStats(4, 4, 1, 1))
  }

  test("min collector") {
    val c = new MinAggregateCollector
    assert(c.result === Nil)
    c.add(newBuffer(3.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(2.0))
    c.add(newBuffer(5.0))
    assert(c.result === List(newBuffer(1.0)))
    assert(c.stats === CollectorStats(4, 4, 1, 1))
  }

  test("max collector") {
    val c = new MaxAggregateCollector
    assert(c.result === Nil)
    c.add(newBuffer(3.0))
    c.add(newBuffer(1.0))
    c.add(newBuffer(2.0))
    c.add(newBuffer(5.0))
    assert(c.result === List(newBuffer(5.0)))
    assert(c.stats === CollectorStats(4, 4, 1, 1))
  }

  test("all collector") {
    val expected = List(
      newBuffer(3.0),
      newBuffer(1.0),
      newBuffer(2.0),
      newBuffer(5.0))
    val c = new AllAggregateCollector
    assert(c.result === Nil)
    expected.foreach(c.add)
    assert(c.result === expected)
    assert(c.stats === CollectorStats(4, 4, 4, 4))
  }

  test("by collector -- all") {
    val expected = List(
      newTaggedBuffer(Map("a" -> "1", "b" -> "2"), 3.0),
      newTaggedBuffer(Map("a" -> "2", "b" -> "2"), 1.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0),
      newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0))
    val by = DataExpr.GroupBy(DataExpr.Sum(Query.False), List("a"))
    val c = new GroupByAggregateCollector(by)
    assert(c.result === Nil)
    expected.foreach(c.add)
    assert(c.result.toSet === expected.toSet)
    assert(c.stats === CollectorStats(4, 4, 4, 4))
  }

  test("by collector -- grp") {
    val input = List(
      newTaggedBuffer(Map("a" -> "1", "b" -> "2"), 3.0),
      newTaggedBuffer(Map("a" -> "2", "b" -> "2"), 1.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0),
      newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0))
    val expected = List(
      newTaggedBuffer(Map("b" -> "2"), 9.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0))
    val by = DataExpr.GroupBy(DataExpr.Sum(Query.False), List("b"))
    val c = new GroupByAggregateCollector(by)
    assert(c.result === Nil)
    input.foreach(c.add)
    assert(c.result.toSet === expected.toSet)
    assert(c.stats === CollectorStats(4, 4, 2, 2))
  }

  test("by collector -- missing key") {
    val input = List(
      newTaggedBuffer(Map("a" -> "1", "b" -> "2"), 3.0),
      newTaggedBuffer(Map("a" -> "2", "b" -> "2"), 1.0),
      newTaggedBuffer(Map("a" -> "3", "b" -> "3"), 2.0),
      newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0))
    val expected = List(
      newTaggedBuffer(Map("a" -> "4", "b" -> "2", "c" -> "7"), 5.0))
    val by = DataExpr.GroupBy(DataExpr.Sum(Query.False), List("b", "c"))
    val c = new GroupByAggregateCollector(by)
    assert(c.result === Nil)
    input.foreach(c.add)
    assert(c.result.toSet === expected.toSet)
    assert(c.stats === CollectorStats(1, 1, 1, 1))
  }
}
