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

import com.netflix.atlas.core.model.Block
import com.netflix.atlas.core.model.BlockStats
import com.netflix.atlas.core.model.CompressedArrayBlock
import munit.FunSuite

class MemoryBlockStoreSuite extends FunSuite {

  test("update, new") {
    val bs = new MemoryBlockStore(1, 60, 1)
    bs.update(0, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(2, 2, Block.Sum).toList, List(3.0))
  }

  test("update, 1m step") {
    val bs = new MemoryBlockStore(60000L, 60, 1)
    bs.update(0, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(0L, 2 * 60000L, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(2 * 60000L, 2 * 60000L, Block.Sum).toList, List(3.0))
  }

  test("update, many blocks") {
    val bs = new MemoryBlockStore(1, 1, 4)
    bs.update(0, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(2, 2, Block.Sum).toList, List(3.0))
  }

  test("update, gap in updates") {
    val bs = new MemoryBlockStore(1, 1, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(10, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(10, 12, Block.Sum).toList, List(4.0, 5.0, 6.0))
    val exp = List(
      1.0,
      2.0,
      3.0,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      4.0,
      5.0,
      6.0
    )
    exp
      .zip(bs.fetch(0, 12, Block.Sum))
      .foreach(t => assertEquals(java.lang.Double.compare(t._1, t._2), 0))
  }

  test("update, gap in updates misalign") {
    val bs = new MemoryBlockStore(1, 4, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(10, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(10, 12, Block.Sum).toList, List(4.0, 5.0, 6.0))
    val exp = List(
      1.0,
      2.0,
      3.0,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      4.0,
      5.0,
      6.0
    )
    exp
      .zip(bs.fetch(0, 12, Block.Sum))
      .foreach(t => assertEquals(java.lang.Double.compare(t._1, t._2), 0))
  }

  test("update, old data") {
    val bs = new MemoryBlockStore(1, 1, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(0, List(4.0, 5.0))
    // previous block can still be updated, but older updates will be ignored
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 5.0, 3.0))
  }

  test("update, old data hour transition") {
    val bs = new MemoryBlockStore(1, 60, 3)
    val input = (0 to 61).map(_.toDouble).toList
    bs.update(0, input)
    assertEquals(bs.fetch(0, 61, Block.Sum).toList, input)

    bs.update(58, 60.0)
    bs.update(59, 60.0)
    assertEquals(bs.fetch(58, 61, Block.Sum).toList, List(60.0, 60.0, 60.0, 61.0))
  }

  test("update, overwrite") {
    val bs = new MemoryBlockStore(1, 60, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(1, List(4.0, 5.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 4.0, 5.0))
  }

  test("update, skip some") {
    val bs = new MemoryBlockStore(1, 10, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(8, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(8, 10, Block.Sum).toList, List(4.0, 5.0, 6.0))
    assert(bs.fetch(3, 7, Block.Sum).forall(_.isNaN))
  }

  test("update, bulk update") {
    val n = 100
    val twoWeeks = 60 * 24 * 14
    val data = (0 until twoWeeks).map(_ => 0.0).toList
    (0 until n).foreach(_ => {
      val bs = new MemoryBlockStore(1, 60, 24 * 14)
      bs.update(0, data)
    })
  }

  test("update, alignment") {
    val bs = new MemoryBlockStore(1, 7, 3)
    bs.update(5, List(1.0, 2.0, 3.0))
    assert(bs.fetch(0, 4, Block.Sum).forall(_.isNaN))
    assertEquals(bs.fetch(5, 7, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assert(bs.fetch(8, 13, Block.Sum).forall(_.isNaN))
    assertEquals(bs.blocks(0), null)
    assertEquals(bs.blocks(1).start, 0L)
    assertEquals(bs.blocks(2).start, 7L)
  }

  test("update, overwrite oldest blocks") {
    val bs = new MemoryBlockStore(1, 2, 3)
    bs.update(0, List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0))
    assert(bs.fetch(0, 1, Block.Sum).forall(_.isNaN))
    assertEquals(bs.fetch(2, 6, Block.Sum).toList, List(3.0, 4.0, 5.0, 6.0, 7.0))
    assert(bs.fetch(7, 10, Block.Sum).forall(_.isNaN))
  }

  test("update, with no data") {
    val bs = new MemoryBlockStore(1, 10, 4)
    (0 until 10).foreach { i =>
      bs.update(i, 2.0)
    }
    assert(bs.currentBlock != null)
    bs.update(0)
    assertEquals(bs.currentBlock, null)
    val expected = CompressedArrayBlock(0, 10)
    (0 until 10).foreach { i =>
      expected.update(i, 2.0)
    }
    assertEquals(bs.blockList, List(expected))
  }

  test("cleanup, nothing to do") {
    val bs = new MemoryBlockStore(1, 60, 1)
    assert(!bs.hasData)
    bs.update(0, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    bs.cleanup(0)
    assert(bs.hasData)
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
  }

  test("cleanup, all") {
    val bs = new MemoryBlockStore(1, 60, 1)
    bs.update(0, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    bs.cleanup(10)
    assert(!bs.hasData)
  }

  test("cleanup, partial") {
    val bs = new MemoryBlockStore(1, 1, 60)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(120, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(0, 2, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(120, 122, Block.Sum).toList, List(4.0, 5.0, 6.0))
    bs.cleanup(10)
    assert(bs.hasData)
    assert(bs.fetch(0, 2, Block.Sum).forall(_.isNaN))
    assertEquals(bs.fetch(120, 122, Block.Sum).toList, List(4.0, 5.0, 6.0))
  }

  test("cleanup, flush data") {
    val step = 60_000L
    val windowSize = step * 60 * 6

    val baseTime = 2880 * step
    val blockStore = new MemoryBlockStore(step, 60, 6)
    val blockMillis = 60 * step
    var nextFlushTime = baseTime / blockMillis * blockMillis
    (0 until 1440).foreach { i =>
      val t = baseTime + i * step
      val boundary = t / blockMillis * blockMillis
      if (boundary > nextFlushTime) {
        blockStore.update(nextFlushTime)
        nextFlushTime = boundary
      }
      blockStore.update(t, i)
    }

    val end = baseTime + 1440 * step - step
    val start = end - windowSize + step
    val data = blockStore.fetch(start, end, Block.Sum)
    data.indices.foreach { i =>
      assertEquals(data(i), 1080.0 + i)
    }
  }

  test("block counts update correctly on roll over") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(60, List(1.0, 2.0, 3.0))
    bs.update(120, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 3)

    bs.update(180, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 3)
  }

  test("block counts update correctly on update(ts)") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 1)

    bs.update(0)
    assertEquals(BlockStats.overallCount, 1)
  }

  test("block counts update correctly on cleanup(ts)") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 1)

    bs.cleanup(60)
    assertEquals(BlockStats.overallCount, 0)
  }

  test("block counts update for update with gap") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(62, List(1.0))
    assertEquals(BlockStats.overallCount, 2)
  }
}
