/*
 * Copyright 2014-2019 Netflix, Inc.
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
import com.netflix.atlas.core.model.ConstantBlock
import org.scalatest.funsuite.AnyFunSuite

class MemoryBlockStoreSuite extends AnyFunSuite {

  test("update, new") {
    val bs = new MemoryBlockStore(1, 60, 1)
    bs.update(0, List(1.0, 2.0, 3.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(2, 2, Block.Sum).toList === List(3.0))
  }

  test("update, 1m step") {
    val bs = new MemoryBlockStore(60000L, 60, 1)
    bs.update(0, List(1.0, 2.0, 3.0))
    assert(bs.fetch(0L, 2 * 60000L, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(2 * 60000L, 2 * 60000L, Block.Sum).toList === List(3.0))
  }

  test("update, many blocks") {
    val bs = new MemoryBlockStore(1, 1, 4)
    bs.update(0, List(1.0, 2.0, 3.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(2, 2, Block.Sum).toList === List(3.0))
  }

  test("update, gap in updates") {
    val bs = new MemoryBlockStore(1, 1, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(10, List(4.0, 5.0, 6.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(10, 12, Block.Sum).toList === List(4.0, 5.0, 6.0))
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
      .foreach(t => assert(java.lang.Double.compare(t._1, t._2) === 0))
  }

  test("update, gap in updates misalign") {
    val bs = new MemoryBlockStore(1, 4, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(10, List(4.0, 5.0, 6.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(10, 12, Block.Sum).toList === List(4.0, 5.0, 6.0))
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
      .foreach(t => assert(java.lang.Double.compare(t._1, t._2) === 0))
  }

  test("update, old data") {
    val bs = new MemoryBlockStore(1, 1, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    intercept[IllegalArgumentException] {
      bs.update(0, List(4.0, 5.0))
    }
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
  }

  test("update, overwrite") {
    val bs = new MemoryBlockStore(1, 60, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(1, List(4.0, 5.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 4.0, 5.0))
  }

  test("update, skip some") {
    val bs = new MemoryBlockStore(1, 10, 40)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(8, List(4.0, 5.0, 6.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(8, 10, Block.Sum).toList === List(4.0, 5.0, 6.0))
    assert(bs.fetch(3, 7, Block.Sum).forall(_.isNaN))
  }

  test("update, bulk update") {
    val n = 100
    val twoWeeks = 60 * 24 * 14
    val data = (0 until twoWeeks).map(v => 0.0).toList
    (0 until n).foreach(i => {
      val bs = new MemoryBlockStore(1, 60, 24 * 14)
      bs.update(0, data)
    })
  }

  test("update, alignment") {
    val bs = new MemoryBlockStore(1, 7, 3)
    bs.update(5, List(1.0, 2.0, 3.0))
    assert(bs.fetch(0, 4, Block.Sum).forall(_.isNaN))
    assert(bs.fetch(5, 7, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(8, 13, Block.Sum).forall(_.isNaN))
    assert(bs.blocks(0) === null)
    assert(bs.blocks(1).start === 0)
    assert(bs.blocks(2).start === 7)
  }

  test("update, overwrite oldest blocks") {
    val bs = new MemoryBlockStore(1, 2, 3)
    bs.update(0, List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0))
    assert(bs.fetch(0, 1, Block.Sum).forall(_.isNaN))
    assert(bs.fetch(2, 6, Block.Sum).toList === List(3.0, 4.0, 5.0, 6.0, 7.0))
    assert(bs.fetch(7, 10, Block.Sum).forall(_.isNaN))
  }

  test("update, with no data") {
    val bs = new MemoryBlockStore(1, 10, 4)
    (0 until 10).foreach { i =>
      bs.update(i, 2.0)
    }
    assert(bs.currentBlock !== null)
    bs.update(0)
    assert(bs.currentBlock === null)
    assert(bs.blockList === List(ConstantBlock(0, 10, 2.0)))
  }

  test("cleanup, nothing to do") {
    val bs = new MemoryBlockStore(1, 60, 1)
    assert(!bs.hasData)
    bs.update(0, List(1.0, 2.0, 3.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    bs.cleanup(0)
    assert(bs.hasData)
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
  }

  test("cleanup, all") {
    val bs = new MemoryBlockStore(1, 60, 1)
    bs.update(0, List(1.0, 2.0, 3.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    bs.cleanup(10)
    assert(!bs.hasData)
  }

  test("cleanup, partial") {
    val bs = new MemoryBlockStore(1, 1, 60)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(120, List(4.0, 5.0, 6.0))
    assert(bs.fetch(0, 2, Block.Sum).toList === List(1.0, 2.0, 3.0))
    assert(bs.fetch(120, 122, Block.Sum).toList === List(4.0, 5.0, 6.0))
    bs.cleanup(10)
    assert(bs.hasData)
    assert(bs.fetch(0, 2, Block.Sum).forall(_.isNaN))
    assert(bs.fetch(120, 122, Block.Sum).toList === List(4.0, 5.0, 6.0))
  }

  private def blockCounts: List[Long] = {
    List(BlockStats.arrayCount.get, BlockStats.sparseCount.get, BlockStats.constantCount.get)
  }

  test("block counts update correctly on roll over") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(60, List(1.0, 2.0, 3.0))
    bs.update(120, List(1.0, 2.0, 3.0))
    assert(blockCounts === List(1, 2, 0))

    bs.update(180, List(1.0, 2.0, 3.0))
    assert(blockCounts === List(1, 2, 0))
  }

  test("block counts update correctly on update(ts)") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    assert(blockCounts === List(1, 0, 0))

    bs.update(0)
    assert(blockCounts === List(0, 1, 0))
  }

  test("block counts update correctly on cleanup(ts)") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    assert(blockCounts === List(1, 0, 0))

    bs.cleanup(60)
    assert(blockCounts === List(0, 0, 0))
  }

  test("block counts update for update with gap") {
    BlockStats.clear()

    val bs = new MemoryBlockStore(1, 60, 3)
    bs.update(0, List(1.0, 2.0, 3.0))
    bs.update(62, List(1.0))
    assert(blockCounts === List(1, 1, 0))
  }
}
