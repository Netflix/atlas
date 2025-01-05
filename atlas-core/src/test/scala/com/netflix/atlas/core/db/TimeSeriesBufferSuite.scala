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

import com.netflix.atlas.core.model.ArrayBlock
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.Block
import com.netflix.atlas.core.model.ConsolidationFunction
import com.netflix.atlas.core.model.ConstantBlock
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.util.Math
import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import munit.FunSuite

class TimeSeriesBufferSuite extends FunSuite {

  import java.lang.Double as JDouble

  private val emptyTags = Map.empty[String, String]

  private def newBuffer(
    v: Double,
    step: Long = 60000L,
    start: Long = 0L,
    n: Int = 1
  ): TimeSeriesBuffer = {
    TimeSeriesBuffer(emptyTags, step, start, Array.fill(n)(v))
  }

  test("apply List[Block]") {
    val tags = emptyTags
    val step = 60000L
    val blocks = List(
      ConstantBlock(0 * step, 6, 1.0),
      ConstantBlock(6 * step, 6, 2.0),
      ConstantBlock(18 * step, 6, 4.0)
    )

    val buffer = TimeSeriesBuffer(tags, step, 1 * step, 19 * step, blocks, Block.Sum)
    val m = buffer
    assertEquals(m.step, step)
    assertEquals(m.start, step)
    assert(m.values.take(5).forall(_ == 1.0))
    assert(m.values.slice(5, 11).forall(_ == 2.0))
    assert(m.values.slice(11, 17).forall(v => JDouble.isNaN(v)))
    assert(m.values.drop(17).forall(_ == 4.0))
  }

  test("add Block") {
    val tags = emptyTags
    val step = 60000L
    val blocks = List(
      ConstantBlock(0 * step, 6, 1.0),
      ConstantBlock(6 * step, 6, 2.0),
      ConstantBlock(18 * step, 6, 4.0)
    )

    val buffer = TimeSeriesBuffer(tags, step, 1 * step, 19 * step)
    blocks.foreach { b =>
      buffer.add(b)
    }
    val m = buffer
    assertEquals(m.step, step)
    assertEquals(m.start, step)
    assert(m.values.take(5).forall(_ == 1.0))
    assert(m.values.slice(5, 11).forall(_ == 2.0))
    assert(m.values.slice(11, 17).forall(v => JDouble.isNaN(v)))
    assert(m.values.drop(17).forall(_ == 4.0))
  }

  test("add Block with cf 6") {
    val tags = emptyTags
    val step = 60000L
    val blocks = List(
      ConstantBlock(0 * step, 6, 1.0),
      ConstantBlock(6 * step, 6, 2.0),
      ConstantBlock(18 * step, 6, 4.0)
    )

    val buffer = TimeSeriesBuffer(tags, 6 * step, step, 18 * step)
    blocks.foreach { b =>
      buffer.aggrBlock(b, Block.Sum, ConsolidationFunction.Max, 6, Math.addNaN)
    }
    val m = buffer
    assertEquals(m.step, 6 * step)
    assertEquals(m.start, 0L)
    assert(m.values.take(1).forall(_ == 1.0))
    assert(m.values.slice(1, 2).forall(_ == 2.0))
    assert(m.values.slice(2, 3).forall(v => JDouble.isNaN(v)))
    assert(m.values.drop(3).forall(_ == 4.0))
  }

  test("add Block with step 10s cf 6m") {
    val tags = emptyTags
    val step = 10000L
    val blockSize = 6 * 60 // each block is 1h
    val blocks =
      (0 until 1000).map(i => ConstantBlock(i * blockSize * step, blockSize, 4.0)).toList

    val multiple = 6 * 6
    val consol = multiple * step
    val buffer = TimeSeriesBuffer(tags, consol, consol, 20 * consol)
    blocks.foreach { b =>
      buffer.aggrBlock(b, Block.Sum, ConsolidationFunction.Max, multiple, Math.addNaN)
    }
    val m = buffer
    assertEquals(m.step, consol)
    assertEquals(m.start, consol)
    assert(m.values.forall(_ == 4.0))
  }

  test("add Block with step 50ms cf 30s") {
    val tags = emptyTags
    val step = 50L
    val blockSize = 20 * 60 // each block is 1m
    val blocks =
      (0 until 1000).map(i => ConstantBlock(i * blockSize * step, blockSize, 4.0)).toList

    val multiple = 10 * 60
    val consol = multiple * step
    val buffer = TimeSeriesBuffer(tags, consol, consol, 20 * consol)
    blocks.foreach { b =>
      buffer.aggrBlock(b, Block.Sum, ConsolidationFunction.Max, multiple, Math.addNaN)
    }
    val m = buffer
    assertEquals(m.step, consol)
    assertEquals(m.start, consol)
    assert(m.values.forall(_ == 4.0))
  }

  test("add Block with step 1ms cf 30s") {
    val tags = emptyTags
    val step = 1L
    val blockSize = 1000 * 60 // each block is 1m
    val blocks =
      (0 until 1000).map(i => ConstantBlock(i * blockSize * step, blockSize, 4.0)).toList

    val multiple = 1000 * 30
    val consol = multiple * step
    val buffer = TimeSeriesBuffer(tags, consol, consol, 20 * consol)
    blocks.foreach { b =>
      buffer.aggrBlock(b, Block.Sum, ConsolidationFunction.Max, multiple, Math.addNaN)
    }
    val m = buffer
    assertEquals(m.step, consol)
    assertEquals(m.start, consol)
    assert(m.values.forall(_ == 4.0))
  }

  test("aggregate tags") {
    // No longer done, it will always use the tags from the initial buffer
    val common = Map("a" -> "b", "c" -> "d")
    val t1 = common + ("c" -> "e")
    val t2 = common + ("z" -> "y")
    val b1 = TimeSeriesBuffer(t1, 60000, 0, Array.fill(1)(0.0))
    val b2 = TimeSeriesBuffer(t2, 60000, 0, Array.fill(1)(0.0))
    b1.add(b2)
    assertEquals(b1.tags, t1)
  }

  test("add buffer") {
    val b1 = newBuffer(42.0)
    val b2 = newBuffer(42.0)
    b1.add(b2)
    b1.values.foreach(v => assertEquals(v, 84.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("add buffer, b1=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(42.0)
    b1.add(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("add buffer, b2=NaN") {
    val b1 = newBuffer(42.0)
    val b2 = newBuffer(Double.NaN)
    b1.add(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("add buffer, b1=b2=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(Double.NaN)
    b1.add(b2)
    b1.values.foreach(v => assert(v.isNaN))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("add constant") {
    val b1 = newBuffer(42.0)
    b1.add(42.0)
    b1.values.foreach(v => assertEquals(v, 84.0))
  }

  test("add constant, b1=NaN") {
    val b1 = newBuffer(Double.NaN)
    b1.add(42.0)
    b1.values.foreach(v => assertEquals(v, 42.0))
  }

  test("add constant, v=NaN") {
    val b1 = newBuffer(42.0)
    b1.add(Double.NaN)
    b1.values.foreach(v => assertEquals(v, 42.0))
  }

  test("add buffer, b1=v=NaN") {
    val b1 = newBuffer(Double.NaN)
    b1.add(Double.NaN)
    b1.values.foreach(v => assert(v.isNaN))
  }

  test("subtract buffer") {
    val b1 = newBuffer(84.0)
    val b2 = newBuffer(42.0)
    b1.subtract(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("subtract constant") {
    val b1 = newBuffer(84.0)
    b1.subtract(42.0)
    b1.values.foreach(v => assertEquals(v, 42.0))
  }

  test("multiply buffer") {
    val b1 = newBuffer(84.0)
    val b2 = newBuffer(2.0)
    b1.multiply(b2)
    b1.values.foreach(v => assertEquals(v, 168.0))
    b2.values.foreach(v => assertEquals(v, 2.0))
  }

  test("multiply constant") {
    val b1 = newBuffer(84.0)
    b1.multiply(2.0)
    b1.values.foreach(v => assertEquals(v, 168.0))
  }

  test("divide buffer") {
    val b1 = newBuffer(84.0)
    val b2 = newBuffer(2.0)
    b1.divide(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assertEquals(v, 2.0))
  }

  test("divide constant") {
    val b1 = newBuffer(84.0)
    b1.divide(2.0)
    b1.values.foreach(v => assertEquals(v, 42.0))
  }

  test("max buffer, b1 > b2") {
    val b1 = newBuffer(42.0)
    val b2 = newBuffer(21.0)
    b1.max(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assertEquals(v, 21.0))
  }

  test("max buffer, b1 < b2") {
    val b1 = newBuffer(21.0)
    val b2 = newBuffer(42.0)
    b1.max(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("max buffer, b1=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(42.0)
    b1.max(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("max buffer, b2=NaN") {
    val b1 = newBuffer(42.0)
    val b2 = newBuffer(Double.NaN)
    b1.max(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("max buffer, b1=b2=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(Double.NaN)
    b1.max(b2)
    b1.values.foreach(v => assert(v.isNaN))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("min buffer, b1 > b2") {
    val b1 = newBuffer(42.0)
    val b2 = newBuffer(21.0)
    b1.min(b2)
    b1.values.foreach(v => assertEquals(v, 21.0))
    b2.values.foreach(v => assertEquals(v, 21.0))
  }

  test("min buffer, b1 < b2") {
    val b1 = newBuffer(21.0)
    val b2 = newBuffer(42.0)
    b1.min(b2)
    b1.values.foreach(v => assertEquals(v, 21.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("min buffer, b1=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(42.0)
    b1.min(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("min buffer, b2=NaN") {
    val b1 = newBuffer(42.0)
    val b2 = newBuffer(Double.NaN)
    b1.min(b2)
    b1.values.foreach(v => assertEquals(v, 42.0))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("min buffer, b1=b2=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(Double.NaN)
    b1.min(b2)
    b1.values.foreach(v => assert(v.isNaN))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("count buffer, b1 < b2") {
    val b1 = newBuffer(21.0)
    val b2 = newBuffer(42.0)
    b1.initCount()
    b1.count(b2)
    b1.values.foreach(v => assertEquals(v, 2.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("count buffer, b1=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(42.0)
    b1.initCount()
    b1.count(b2)
    b1.values.foreach(v => assertEquals(v, 1.0))
    b2.values.foreach(v => assertEquals(v, 42.0))
  }

  test("count buffer, b2=NaN") {
    val b1 = newBuffer(42.0)
    val b2 = newBuffer(Double.NaN)
    b1.initCount()
    b1.count(b2)
    b1.values.foreach(v => assertEquals(v, 1.0))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("count buffer, b1=b2=NaN") {
    val b1 = newBuffer(Double.NaN)
    val b2 = newBuffer(Double.NaN)
    b1.initCount()
    b1.count(b2)
    b1.values.foreach(v => assertEquals(v, 0.0))
    b2.values.foreach(v => assert(v.isNaN))
  }

  test("getValue prior to start") {
    val b1 = newBuffer(42.0, start = 120000)
    assert(b1.getValue(60000).isNaN)
  }

  test("getValue after the end") {
    val b1 = newBuffer(42.0, start = 120000)
    assert(b1.getValue(240000).isNaN)
  }

  test("getValue with match") {
    val b1 = newBuffer(42.0, start = 120000)
    assertEquals(b1.getValue(129000), 42.0)
  }

  test("sum") {
    val buffers = List(newBuffer(1.0), newBuffer(Double.NaN), newBuffer(2.0))
    assertEquals(TimeSeriesBuffer.sum(buffers).get.values(0), 3.0)
  }

  test("sum empty") {
    assertEquals(TimeSeriesBuffer.sum(Nil), None)
  }

  test("max") {
    val buffers = List(newBuffer(1.0), newBuffer(Double.NaN), newBuffer(2.0))
    assertEquals(TimeSeriesBuffer.max(buffers).get.values(0), 2.0)
  }

  test("max empty") {
    assertEquals(TimeSeriesBuffer.max(Nil), None)
  }

  test("min") {
    val buffers = List(newBuffer(1.0), newBuffer(Double.NaN), newBuffer(2.0))
    assertEquals(TimeSeriesBuffer.min(buffers).get.values(0), 1.0)
  }

  test("min empty") {
    assertEquals(TimeSeriesBuffer.min(Nil), None)
  }

  test("count") {
    val buffers = List(newBuffer(1.0), newBuffer(Double.NaN), newBuffer(2.0))
    assertEquals(TimeSeriesBuffer.count(buffers).get.values(0), 2.0)
  }

  test("count empty") {
    assertEquals(TimeSeriesBuffer.count(Nil), None)
  }

  test("merge diff sizes b1.length < b2.length") {
    val b1 = newBuffer(1.0, n = 1)
    val b2 = newBuffer(2.0, n = 2)
    b1.merge(b2)
    assert(b1.values(0) == 2.0)
  }

  test("merge diff sizes b1.length > b2.length") {
    val b1 = newBuffer(7.0, n = 1)
    val b2 = newBuffer(2.0, n = 2)
    b2.merge(b1)
    assert(b2.values(0) == 7.0)
    assert(b2.values(1) == 2.0)
  }

  test("consolidate") {
    val start = 1366746900000L
    val b = TimeSeriesBuffer(emptyTags, 60000, start, Array(1.0, 2.0, 3.0, 4.0, 5.0))

    val b2 = TimeSeriesBuffer(emptyTags, 120000, start, Array(1.0, 5.0, 9.0))
    assertEquals(b.consolidate(2, ConsolidationFunction.Sum), b2)

    val b3 = TimeSeriesBuffer(emptyTags, 180000, start, Array(3.0, 12.0))
    assertEquals(b.consolidate(3, ConsolidationFunction.Sum), b3)

    val b4 = TimeSeriesBuffer(emptyTags, 240000, start, Array(1.0, 14.0))
    assertEquals(b.consolidate(4, ConsolidationFunction.Sum), b4)

    val b5 = TimeSeriesBuffer(emptyTags, 300000, start, Array(15.0))
    assertEquals(b.consolidate(5, ConsolidationFunction.Sum), b5)
  }

  test("consolidate NaN, avg with a rate") {
    val start = 1366746900000L
    val tags = Map(TagKey.dsType -> "rate")
    val b = TimeSeriesBuffer(tags, 60000, start, Array(1.0, 2.0, Double.NaN, 4.0, 5.0))

    val b5 = TimeSeriesBuffer(tags, 300000, start, Array(12.0 / 5.0))
    assertEquals(b.consolidate(5, ConsolidationFunction.Avg), b5)
  }

  test("consolidate NaN, avg with gauge") {
    val start = 1366746900000L
    val tags = Map(TagKey.dsType -> "gauge")
    val b = TimeSeriesBuffer(tags, 60000, start, Array(1.0, 2.0, Double.NaN, 4.0, 5.0))

    val b5 = TimeSeriesBuffer(tags, 300000, start, Array(12.0 / 4.0))
    assertEquals(b.consolidate(5, ConsolidationFunction.Avg), b5)
  }

  test("normalize rate") {
    val start = 1366746900000L
    val b1 = TimeSeriesBuffer(emptyTags, 60000, start, Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val b1e = TimeSeriesBuffer(emptyTags, 120000, start, Array(0.5, 2.5, 4.5))
    assertEquals(b1.normalize(60000, start, 5), b1)
    assertEquals(b1.normalize(120000, start, 3), b1e)

    val b2 = TimeSeriesBuffer(emptyTags, 120000, start, Array(3.0, 7.0))
    val b2e =
      TimeSeriesBuffer(emptyTags, 60000, start, Array(3.0, 7.0, 7.0, Double.NaN, Double.NaN))
    assertEquals(b2.normalize(60000, start, 5), b2e)
  }

  test("normalize gauge") {
    val start = 1366746900000L
    val tags = Map(TagKey.dsType -> "gauge")
    val b1 = TimeSeriesBuffer(tags, 60000, start, Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val b1e = TimeSeriesBuffer(tags, 120000, start, Array(1.0, 2.5, 4.5))
    assertEquals(b1.normalize(60000, start, 5), b1)
    assertEquals(b1.normalize(120000, start, 3), b1e)

    val b2 = TimeSeriesBuffer(tags, 120000, start, Array(3.0, 7.0))
    val b2e =
      TimeSeriesBuffer(tags, 60000, start, Array(3.0, 7.0, 7.0, Double.NaN, Double.NaN))
    assertEquals(b2.normalize(60000, start, 5), b2e)
  }

  test("bug: consolidated aggr using incorrect block index") {
    val start = 1407510000000L
    val block = ArrayBlock(start, 60)
    (0 until 60).foreach { i =>
      block.buffer(i) = if (i == 9) 12.0 else 0.0
    }

    val step = 300000L
    val bufStart = start + step * 4
    val end = bufStart + step * 12
    val buffer = TimeSeriesBuffer(emptyTags, step, bufStart, end)

    buffer.aggrBlock(block, Block.Sum, ConsolidationFunction.Avg, 5, Math.addNaN)
    buffer.values.foreach { v =>
      assert(v.isNaN || v <= 0.0)
    }
  }

  test("equals") {
    // https://groups.google.com/forum/#!topic/equalsverifier/R5MWUGVx-C8

    val t1 = Map("a" -> "1")
    val t2 = Map("a" -> "2")

    val step = 60000L
    val s1 = new ArrayTimeSeq(DsType.Gauge, 5 * step, step, Array(1.0))
    val s2 = new ArrayTimeSeq(DsType.Gauge, 5 * step, step, Array(1.0, 2.0))

    // Lazy val for id under 2.13.x will have two fields in the class file, `id`
    // and `bitmap$0`. Under 3.3.x the field changed to `id$lzy1`.
    val lazyIdFields =
      try {
        classOf[TimeSeriesBuffer].getDeclaredField("id$lzy1")
        Array("id$lzy1")
      } catch {
        case _: Exception => Array("id", "bitmap$0")
      }

    EqualsVerifier
      .forClass(classOf[TimeSeriesBuffer])
      .withPrefabValues(classOf[Map[?, ?]], t1, t2)
      .withPrefabValues(classOf[ArrayTimeSeq], s1, s2)
      .withIgnoredFields(lazyIdFields*)
      .suppress(Warning.NULL_FIELDS)
      .suppress(Warning.NONFINAL_FIELDS)
      .verify()
  }
}
