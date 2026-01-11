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

import com.netflix.atlas.core.model.Block
import com.netflix.atlas.core.model.BlockStats
import com.netflix.atlas.core.model.CompressedArrayBlock
import munit.FunSuite

class MemoryBlockStoreSuite extends FunSuite {

  test("update, new") {
    val bs = new MemoryBlockStore(1, 60, 1)
    bs.update(1, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(3, 3, Block.Sum).toList, List(3.0))
  }

  test("update, 1m step") {
    val step = 60_000L
    val bs = new MemoryBlockStore(step, 60, 1)
    bs.update(step, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(step, 3 * step, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(3 * step, 3 * step, Block.Sum).toList, List(3.0))
  }

  test("update, many blocks") {
    val bs = new MemoryBlockStore(1, 1, 4)
    bs.update(1, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(3, 3, Block.Sum).toList, List(3.0))
  }

  test("update, gap in updates") {
    val bs = new MemoryBlockStore(1, 1, 40)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(11, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(11, 13, Block.Sum).toList, List(4.0, 5.0, 6.0))
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
      .zip(bs.fetch(1, 13, Block.Sum))
      .foreach(t => assertEquals(java.lang.Double.compare(t._1, t._2), 0))
  }

  test("update, gap in updates misalign") {
    val bs = new MemoryBlockStore(1, 4, 40)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(11, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(11, 13, Block.Sum).toList, List(4.0, 5.0, 6.0))
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
      .zip(bs.fetch(1, 13, Block.Sum))
      .foreach(t => assertEquals(java.lang.Double.compare(t._1, t._2), 0))
  }

  test("update, old data") {
    val bs = new MemoryBlockStore(1, 1, 40)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(1, List(4.0, 5.0))
    // previous block can still be updated, but older updates will be ignored
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 5.0, 3.0))
  }

  test("update, old data hour transition") {
    val bs = new MemoryBlockStore(1, 60, 3)
    val input = (1 to 62).map(_.toDouble).toList
    bs.update(1, input)
    assertEquals(bs.fetch(1, 62, Block.Sum).toList, input)

    bs.update(59, 60.0)
    bs.update(61, 60.0)
    assertEquals(bs.fetch(59, 62, Block.Sum).toList, List(60.0, 60.0, 60.0, 62.0))
  }

  test("update, overwrite") {
    val bs = new MemoryBlockStore(1, 60, 40)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(2, List(4.0, 5.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 4.0, 5.0))
  }

  test("update, skip some") {
    val bs = new MemoryBlockStore(1, 10, 40)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(9, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(9, 11, Block.Sum).toList, List(4.0, 5.0, 6.0))
    assert(bs.fetch(4, 8, Block.Sum).forall(_.isNaN))
  }

  test("update, bulk update") {
    val n = 100
    val twoWeeks = 60 * 24 * 14
    val data = (0 until twoWeeks).map(_ => 0.0).toList
    (0 until n).foreach(_ => {
      val bs = new MemoryBlockStore(1, 60, 24 * 14)
      bs.update(1, data)
    })
  }

  test("update, alignment") {
    val bs = new MemoryBlockStore(1, 7, 3)
    bs.update(6, List(1.0, 2.0, 3.0))
    assert(bs.fetch(1, 5, Block.Sum).forall(_.isNaN))
    assertEquals(bs.fetch(6, 8, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assert(bs.fetch(9, 14, Block.Sum).forall(_.isNaN))
    assertEquals(bs.blocks(0), null)
    assertEquals(bs.blocks(1).start, 1L)
    assertEquals(bs.blocks(2).start, 8L)
  }

  test("update, overwrite oldest blocks") {
    val bs = new MemoryBlockStore(1, 2, 3)
    bs.update(1, List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0))
    assert(bs.fetch(1, 2, Block.Sum).forall(_.isNaN))
    assertEquals(bs.fetch(3, 7, Block.Sum).toList, List(3.0, 4.0, 5.0, 6.0, 7.0))
    assert(bs.fetch(8, 11, Block.Sum).forall(_.isNaN))
  }

  test("update, with no data") {
    val bs = new MemoryBlockStore(1, 10, 4)
    (1 until 11).foreach { i =>
      bs.update(i, 2.0)
    }
    assert(bs.currentBlock != null)
    bs.update(1)
    assertEquals(bs.currentBlock, null)
    val expected = CompressedArrayBlock(1, 10)
    (1 until 11).foreach { i =>
      expected.update(i - 1, 2.0)
    }
    assertEquals(bs.blockList, List(expected))
  }

  test("cleanup, nothing to do") {
    val bs = new MemoryBlockStore(1, 60, 1)
    assert(!bs.hasData)
    bs.update(1, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    bs.cleanup(1)
    assert(bs.hasData)
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
  }

  test("cleanup, all") {
    val bs = new MemoryBlockStore(1, 60, 1)
    bs.update(1, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    bs.cleanup(11)
    assert(!bs.hasData)
  }

  test("cleanup, partial") {
    val bs = new MemoryBlockStore(1, 1, 60)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(121, List(4.0, 5.0, 6.0))
    assertEquals(bs.fetch(1, 3, Block.Sum).toList, List(1.0, 2.0, 3.0))
    assertEquals(bs.fetch(121, 123, Block.Sum).toList, List(4.0, 5.0, 6.0))
    bs.cleanup(11)
    assert(bs.hasData)
    assert(bs.fetch(1, 3, Block.Sum).forall(_.isNaN))
    assertEquals(bs.fetch(121, 123, Block.Sum).toList, List(4.0, 5.0, 6.0))
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
      if (i == 0)
        assert(data(i).isNaN) // data cleaned up
      else
        assertEquals(data(i), 1080.0 + i)
    }
  }

  test("block counts update correctly on roll over") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(61, List(1.0, 2.0, 3.0))
    bs.update(121, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 3)

    bs.update(181, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 3)
  }

  test("block counts update correctly on update(ts)") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(1, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 1)

    bs.update(1)
    assertEquals(BlockStats.overallCount, 1)
  }

  test("block counts update correctly on cleanup(ts)") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(1, List(1.0, 2.0, 3.0))
    assertEquals(BlockStats.overallCount, 1)

    bs.cleanup(61)
    assertEquals(BlockStats.overallCount, 0)
  }

  test("block counts update for update with gap") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(1, List(1.0, 2.0, 3.0))
    bs.update(63, List(1.0))
    assertEquals(BlockStats.overallCount, 2)
  }

  test("align start maps correctly") {
    val step = 60_000L
    val bs = new MemoryBlockStore(step, 60, 6)
    (step to step * 60 by step).foreach { t =>
      assertEquals(bs.alignStart(t), step)
    }
    (step * 60 + step to step * 120 by step).foreach { t =>
      assertEquals(bs.alignStart(t), step * 60 + step)
    }
  }
}
