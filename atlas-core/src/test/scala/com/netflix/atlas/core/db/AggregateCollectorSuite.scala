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

  private def sumCollectorTest(
    blockStart: Long,
    bufferStart: Long,
    multiple: Int
  ): List[TimeSeriesBuffer] = {
    // Reproduces: ArrayIndexOutOfBoundsException: Index -3 out of bounds for length 60
    // This occurs when consolidating data with multiple > 1 and the buffer start
    // aligns in a specific way with the block start.
    import com.netflix.atlas.core.model.ArrayBlock
    import com.netflix.atlas.core.model.Block
    import com.netflix.atlas.core.model.ConsolidationFunction

    val c = new SumAggregateCollector
    val tags = Map("name" -> "test")

    // Create a block with start=0 and 60 values
    val block = ArrayBlock(blockStart, 60)
    // Fill the block with test data
    var i = 0
    while (i < 60) {
      block.update(i, 1.0)
      i += 1
    }
    val blocks = List(block)

    // Buffer parameters: start=0, step=3m
    // When multiple=3, this causes j to become negative
    val bufferStep = 60_000L * multiple

    val newBuffer = (t: Map[String, String]) => {
      val data = new Array[Double](60)
      java.util.Arrays.fill(data, Double.NaN)
      new TimeSeriesBuffer(
        t,
        new ArrayTimeSeq(DsType.Gauge, bufferStart, bufferStep, data)
      )
    }

    // This should trigger the ArrayIndexOutOfBoundsException with index -3
    c.add(tags, blocks, Block.Sum, ConsolidationFunction.Sum, multiple, newBuffer)

    c.result
  }

  test("sum collector -- blockStart=0, bufferStart=0, m=1") {
    val buffers = sumCollectorTest(0L, 0L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    // Block has 60 values, buffer has 60 positions, should have 60 values of 1.0
    buffer.values.foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
  }

  test("sum collector -- blockStart=0, bufferStart=0, m=3".fail) {
    // This case triggers ArrayIndexOutOfBoundsException: Index -3
    val buffers = sumCollectorTest(0L, 0L, 3)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    // With consolidation of 3, each buffer position sums 3 block values
    buffer.values.foreach(v => assertEqualsDouble(v, 3.0, 1e-12))
  }

  test("sum collector -- blockStart=0, bufferStart=step") {
    val buffers = sumCollectorTest(0L, 60_000L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(59).foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
    buffer.values.drop(59).foreach(v => assert(v.isNaN))
  }

  test("sum collector -- blockStart=primaryStep, bufferStart=0, m=1") {
    val buffers = sumCollectorTest(60_000L, 0L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(1).foreach(v => assert(v.isNaN))
    buffer.values.drop(1).foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
  }

  test("sum collector -- blockStart=primaryStep, bufferStart=0, m=3") {
    val buffers = sumCollectorTest(60_000L, 0L, 3)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(1).foreach(v => assert(v.isNaN))
    buffer.values.slice(1, 21).foreach(v => assertEqualsDouble(v, 3.0, 1e-12))
    buffer.values.drop(21).foreach(v => assert(v.isNaN))
  }

  test("sum collector -- blockStart=primaryStep, bufferStart=step, m=2") {
    val buffers = sumCollectorTest(60_000L, 120_000L, 2)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    // Block: 60_000 to 3_660_000 (60 * 60_000)
    // Buffer: 120_000 to 7_320_000 (60 * 120_000)
    // With m=2, buffer step is 120_000
    // First 30 positions should have values
    buffer.values.take(30).foreach(v => assertEqualsDouble(v, 2.0, 1e-12))
    // Rest should be NaN
    buffer.values.drop(30).foreach(v => assert(v.isNaN))
  }

  test("sum collector -- blockStart=primaryStep, bufferStart=step, m=3") {
    // May trigger negative index with consolidation
    val buffers = sumCollectorTest(60_000L, 180_000L, 3)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    // Block: 60_000 to 3_660_000
    // Buffer: 180_000 to 10_980_000 (60 * 180_000)
    // With m=3, buffer step is 180_000
    buffer.values.take(20).foreach(v => assertEqualsDouble(v, 3.0, 1e-12))
    buffer.values.drop(20).foreach(v => assert(v.isNaN))
  }

  test("sum collector -- blockStart=step, bufferStart=step") {
    val buffers = sumCollectorTest(60_000L, 60_000L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
  }

  private def avgCollectorTest(
    blockStart: Long,
    bufferStart: Long,
    multiple: Int
  ): List[TimeSeriesBuffer] = {
    // Reproduces: ArrayIndexOutOfBoundsException: Index -3 out of bounds for length 60
    // This occurs when consolidating data with multiple > 1 and the buffer start
    // aligns in a specific way with the block start.
    import com.netflix.atlas.core.model.ArrayBlock
    import com.netflix.atlas.core.model.Block
    import com.netflix.atlas.core.model.ConsolidationFunction

    val c = new SumAggregateCollector
    val tags = Map("name" -> "test")

    // Create a block with start=0 and 60 values
    val block = ArrayBlock(blockStart, 60)
    // Fill the block with test data
    var i = 0
    while (i < 60) {
      block.update(i, 1.0)
      i += 1
    }
    val blocks = List(block)

    // Buffer parameters: start=0, step=3m
    // When multiple=3, this causes j to become negative
    val bufferStep = 60_000L * multiple

    val newBuffer = (t: Map[String, String]) => {
      val data = new Array[Double](60)
      java.util.Arrays.fill(data, Double.NaN)
      new TimeSeriesBuffer(
        t,
        new ArrayTimeSeq(DsType.Gauge, bufferStart, bufferStep, data)
      )
    }

    // This should trigger the ArrayIndexOutOfBoundsException with index -3
    c.add(tags, blocks, Block.Sum, ConsolidationFunction.Avg, multiple, newBuffer)

    c.result
  }

  test("avg collector -- blockStart=0, bufferStart=0, m=1") {
    val buffers = avgCollectorTest(0L, 0L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
  }

  test("avg collector -- blockStart=0, bufferStart=0, m=3".fail) {
    val buffers = avgCollectorTest(0L, 0L, 3)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.foreach(v => assertEqualsDouble(v, 3.0, 1e-12))
  }

  test("avg collector -- blockStart=0, bufferStart=step") {
    val buffers = avgCollectorTest(0L, 60_000L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(59).foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
    buffer.values.drop(59).foreach(v => assert(v.isNaN))
  }

  test("avg collector -- blockStart=primaryStep, bufferStart=0, m=1") {
    val buffers = avgCollectorTest(60_000L, 0L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(1).foreach(v => assert(v.isNaN))
    buffer.values.drop(1).foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
  }

  test("avg collector -- blockStart=primaryStep, bufferStart=0, m=3") {
    val buffers = avgCollectorTest(60_000L, 0L, 3)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(1).foreach(v => assert(v.isNaN))
    buffer.values.slice(1, 21).foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
    buffer.values.drop(21).foreach(v => assert(v.isNaN))
  }

  test("avg collector -- blockStart=primaryStep, bufferStart=step, m=2") {
    val buffers = avgCollectorTest(60_000L, 120_000L, 2)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(30).foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
    buffer.values.drop(30).foreach(v => assert(v.isNaN))
  }

  test("avg collector -- blockStart=primaryStep, bufferStart=step, m=3") {
    val buffers = avgCollectorTest(60_000L, 180_000L, 3)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.take(20).foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
    buffer.values.drop(20).foreach(v => assert(v.isNaN))
  }

  test("avg collector -- blockStart=step, bufferStart=step") {
    val buffers = avgCollectorTest(60_000L, 60_000L, 1)
    assertEquals(buffers.size, 1)
    val buffer = buffers.head
    buffer.values.foreach(v => assertEqualsDouble(v, 1.0, 1e-12))
  }
}
