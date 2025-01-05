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
package com.netflix.atlas.core.model

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import munit.FunSuite

import scala.util.Random

class BlockSuite extends FunSuite {

  def rleBlock(start: Long, data: List[(Int, Double)], size: Int = 60): Block = {
    val block = ArrayBlock(start, size)
    var i = 0
    data.foreach { t =>
      (i to t._1).foreach { j =>
        block.buffer(j) = t._2
      }
      i = t._1 + 1
    }
    Block.compress(block)
  }

  def checkValues(b: Block, values: List[Double]): Unit = {
    values.zipWithIndex.foreach { v =>
      val msg = "b(%d) => %f != %f".format(v._2, b.get(v._2), v._1)
      val res = java.lang.Double.compare(v._1, b.get(v._2))
      assert(res == 0, msg)
    }
  }

  test("ConstantBlock.get") {
    val b = ConstantBlock(0L, 60, 42.0)
    checkValues(b, (0 until 60).map(_ => 42.0).toList)
  }

  test("ArrayBlock.get") {
    val b = ArrayBlock(0L, 60)
    (0 until 60).foreach(i => b.buffer(i) = i)
    checkValues(b, (0 until 60).map(i => i.toDouble).toList)
    intercept[ArrayIndexOutOfBoundsException] {
      b.get(60)
    }
  }

  test("RleBlock.get") {
    val data = List(5 -> 42.0, 37 -> Double.NaN, 59 -> 21.0)
    val b = rleBlock(0L, data)
    val ab = b.toArrayBlock
    checkValues(b, ab.buffer.toList)
  }

  test("SparseBlock.get") {
    val data =
      (0 until 5).map(_ => 0) ++
        (5 until 37).map(_ => SparseBlock.NaN) ++
        (37 until 60).map(_ => 1)
    val indexes = data.map(_.asInstanceOf[Byte]).toArray
    val values = Array(42.0, 21.0)
    val b = SparseBlock(0L, indexes, values)
    val ab = b.toArrayBlock
    checkValues(b, ab.buffer.toList)
  }

  test("SparseBlock.get, size > 120") {
    val data =
      (0 until 5).map(_ => 0) ++
        (5 until 37).map(_ => SparseBlock.NaN) ++
        (37 until 360).map(_ => 1)
    val indexes = data.map(_.asInstanceOf[Byte]).toArray
    val values = Array(42.0, 21.0)
    val b = SparseBlock(0L, indexes, values)
    val ab = b.toArrayBlock
    checkValues(b, ab.buffer.toList)
  }

  test("Block.get(pos, aggr)") {
    import java.lang.Double as JDouble
    val b = ArrayBlock(0L, 2)
    b.buffer(0) = 0.0
    b.buffer(1) = Double.NaN
    assertEquals(b.get(0, Block.Sum), 0.0)
    assertEquals(b.get(0, Block.Count), 1.0)
    assertEquals(b.get(0, Block.Min), 0.0)
    assertEquals(b.get(0, Block.Max), 0.0)
    assert(JDouble.isNaN(b.get(1, Block.Sum)))
    assert(JDouble.isNaN(b.get(1, Block.Count)))
    assert(JDouble.isNaN(b.get(1, Block.Min)))
    assert(JDouble.isNaN(b.get(1, Block.Max)))
  }

  test("compress, constant value") {
    val b = ArrayBlock(0L, 60)
    (0 until 60).foreach(i => b.buffer(i) = 42.0)
    assert(Block.compress(b) == ConstantBlock(0L, 60, 42.0))
  }

  test("compress, rle -> sparse") {
    val data = List(5 -> 42.0, 37 -> Double.NaN, 59 -> 21.0)
    val b = rleBlock(0L, data)
    val nb = Block.compress(b.toArrayBlock).asInstanceOf[SparseBlock]
    // assertEquals(nb.byteCount, 84)
    assertEquals(nb.values.length, 2)
    (0 until 60).foreach { i =>
      assertEquals(java.lang.Double.compare(b.get(i), nb.get(i)), 0)
    }
  }

  test("compress, rle -> sparse, special values") {
    val data = List(5 -> 2.0, 37 -> Double.NaN, 59 -> 0.0)
    val b = rleBlock(0L, data)
    val nb = Block.compress(b.toArrayBlock).asInstanceOf[SparseBlock]
    assertEquals(nb.values.length, 1)
    (0 until 60).foreach { i =>
      assertEquals(java.lang.Double.compare(b.get(i), nb.get(i)), 0)
    }
  }

  test("compress, rle -> sparse, large block") {
    val data = List(5 -> 2.0, 37 -> Double.NaN, 359 -> 0.0)
    val b = rleBlock(0L, data, 360)
    val nb = Block.compress(b.toArrayBlock).asInstanceOf[SparseBlock]
    assertEquals(nb.values.length, 1)
    (0 until 360).foreach { i =>
      assertEquals(java.lang.Double.compare(b.get(i), nb.get(i)), 0)
    }
  }

  test("compress fails on large block") {
    val data = (0 until 360).map(i => i -> (if (i <= 129) i.toDouble else 42.42)).toList
    val b = rleBlock(0L, data, 360)
    val nb = Block.compress(b.toArrayBlock)
    (0 until 360).foreach { i =>
      assertEquals(java.lang.Double.compare(b.get(i), nb.get(i)), 0)
    }
  }

  test("compress, array") {
    val b = ArrayBlock(0L, 60)
    (0 until 60).foreach(i => b.buffer(i) = i)
    assert(Block.compress(b) eq b)
  }

  test("lossyCompress, array") {
    val b = ArrayBlock(0L, 60)
    (0 until 60).foreach(i => b.buffer(i) = i)
    assertEquals(Block.lossyCompress(b), FloatArrayBlock(b))
  }

  test("compress, small block") {
    val b = ArrayBlock(0L, 5)
    (0 until 5).foreach(i => b.buffer(i) = 42.0)
    assert(Block.compress(b) eq b)
  }

  test("merge") {
    val data1 = List(5 -> 42.0, 37 -> Double.NaN, 59 -> 21.0)
    val data2 = List(9 -> 41.0, 45 -> Double.NaN, 59 -> 22.0)
    val expected = List(5 -> 42.0, 9 -> 41.0, 37 -> Double.NaN, 45 -> 21.0, 59 -> 22.0)
    val b1 = rleBlock(0L, data1)
    val b2 = rleBlock(0L, data2)
    val b3 = Block.merge(b1, b2)
    val expBlock = rleBlock(0L, expected).toArrayBlock
    (0 until 60).foreach { i =>
      assertEquals(java.lang.Double.compare(b3.get(i), expBlock.get(i)), 0)
    }
    assert(b3.isInstanceOf[SparseBlock])
  }

  test("merge rollup") {
    val b1 = RollupBlock(
      min = ConstantBlock(0L, 60, 1.0),
      max = ConstantBlock(0L, 60, 50.0),
      sum = ConstantBlock(0L, 60, 51.0),
      count = ConstantBlock(0L, 60, 2.0)
    )
    val b2 = RollupBlock(
      min = ConstantBlock(0L, 60, 2.0),
      max = ConstantBlock(0L, 60, 3.0),
      sum = ConstantBlock(0L, 60, 6.0),
      count = ConstantBlock(0L, 60, 3.0)
    )
    val b3 = Block.merge(b1, b2)

    val expected = RollupBlock(
      min = ConstantBlock(0L, 60, 2.0),
      max = ConstantBlock(0L, 60, 50.0),
      sum = ConstantBlock(0L, 60, 51.0),
      count = ConstantBlock(0L, 60, 3.0)
    )
    assertEquals(b3, expected)
  }

  test("merge prefer rollup to scalar") {
    val b1 = RollupBlock(
      min = ConstantBlock(0L, 60, 1.0),
      max = ConstantBlock(0L, 60, 50.0),
      sum = ConstantBlock(0L, 60, 51.0),
      count = ConstantBlock(0L, 60, 2.0)
    )
    val b2 = ConstantBlock(0L, 60, 2.0)
    assertEquals(Block.merge(b1, b2), b1)
    assertEquals(Block.merge(b2, b1), b1)
  }

  test("rollup") {
    import java.lang.Double as JDouble

    val n = 5

    val r = RollupBlock.empty(0L, n)
    (0 until n).foreach { i =>
      assert(JDouble.isNaN(r.get(i, Block.Sum)))
      assert(JDouble.isNaN(r.get(i, Block.Count)))
      assert(JDouble.isNaN(r.get(i, Block.Min)))
      assert(JDouble.isNaN(r.get(i, Block.Max)))
    }

    r.rollup(ConstantBlock(0L, n, 1.0))
    (0 until n).foreach { i =>
      assertEquals(r.get(i, Block.Sum), 1.0)
      assertEquals(r.get(i, Block.Count), 1.0)
      assertEquals(r.get(i, Block.Min), 1.0)
      assertEquals(r.get(i, Block.Max), 1.0)
    }

    r.rollup(ConstantBlock(0L, n, 3.0))
    (0 until n).foreach { i =>
      assertEquals(r.get(i, Block.Sum), 4.0)
      assertEquals(r.get(i, Block.Count), 2.0)
      assertEquals(r.get(i, Block.Min), 1.0)
      assertEquals(r.get(i, Block.Max), 3.0)
    }

    r.rollup(ConstantBlock(0L, n, 0.5))
    (0 until n).foreach { i =>
      assertEquals(r.get(i, Block.Sum), 4.5)
      assertEquals(r.get(i, Block.Count), 3.0)
      assertEquals(r.get(i, Block.Min), 0.5)
      assertEquals(r.get(i, Block.Max), 3.0)
    }
  }

  test("compressed array: get/set 2") {
    import CompressedArrayBlock.*

    (0 until 32).foreach { i =>
      assertEquals(set2(0L, i, 0), 0L)
      assertEquals(set2(0L, i, 1), 1L << (2 * i))
      assertEquals(set2(0L, i, 2), 2L << (2 * i))
      assertEquals(set2(0L, i, 3), 3L << (2 * i))

      (0 until 32).foreach { j =>
        assertEquals(get2(set2(-1L, i, 0), j), if (i == j) 0 else 3)
        assertEquals(get2(set2(-1L, i, 1), j), if (i == j) 1 else 3)
        assertEquals(get2(set2(-1L, i, 2), j), if (i == j) 2 else 3)
        assertEquals(get2(set2(-1L, i, 3), j), 3)
      }
    }
  }

  test("compressed array: get/set 4") {
    import CompressedArrayBlock.*

    (0 until 16).foreach { i =>
      (0 until 16).foreach { v =>
        assertEquals(set4(0L, i, v), v.toLong << (4 * i))
      }

      (0 until 16).foreach { j =>
        (0 until 16).foreach { v =>
          assertEquals(get4(set4(-1L, i, v), j), if (i == j) v else 0xF)
        }
      }
    }
  }

  test("compressed array: ceiling division") {
    import CompressedArrayBlock.*
    assertEquals(ceilingDivide(8, 2), 4)
    assertEquals(ceilingDivide(9, 2), 5)
  }

  test("compressed array: empty") {
    val block = CompressedArrayBlock(0L, 60)
    (0 until 60).foreach { i =>
      assert(block.get(i).isNaN)
    }
  }

  test("compressed array: zero") {
    val block = CompressedArrayBlock(0L, 60)
    (0 until 60).foreach { i =>
      block.update(i, 0.0)
      assertEquals(block.get(i), 0.0)
    }
    assert(block.byteCount < 25)
  }

  test("compressed array: single increment") {
    val block = CompressedArrayBlock(0L, 60)
    block.update(14, 1.0 / 60.0)
    assertEquals(block.get(14), 1.0 / 60.0)
    (15 until 30).foreach { i =>
      block.update(i, 0.0)
      assertEquals(block.get(i), 0.0)
    }
    assert(block.byteCount < 25)
  }

  test("compressed array: double increment") {
    val block = CompressedArrayBlock(0L, 60)
    block.update(14, 2.0 / 60.0)
    assertEquals(block.get(14), 2.0 / 60.0)
    (15 until 30).foreach { i =>
      block.update(i, 0.0)
      assertEquals(block.get(i), 0.0)
    }
    assert(block.byteCount < 50)
  }

  test("compressed array: single uncommon value") {
    val block = CompressedArrayBlock(0L, 60)
    (0 until 60).foreach { i =>
      block.update(i, 2.0)
      assertEquals(block.get(i), 2.0)
    }
    assert(block.byteCount < 50)
  }

  test("compressed array: grow") {
    val block = CompressedArrayBlock(0L, 60)
    (0 until 60).foreach { i =>
      block.update(i, i.toDouble)
      assertEquals(block.get(i), i.toDouble)
    }
    assert(block.byteCount < 500)
  }

  test("compressed array: random access") {
    val block = CompressedArrayBlock(0L, 60)
    Random.shuffle((0 until 60).toList).foreach { i =>
      block.update(i, i.toDouble)
    }
    (0 until 60).foreach { i =>
      assertEquals(block.get(i), i.toDouble)
    }
    assert(block.byteCount < 500)
  }

  test("compressed array: various block sizes") {
    (1 until 24 * 60).foreach { i =>
      val block = CompressedArrayBlock(0L, i)
      (0 until i).foreach { j =>
        block.update(j, j.toDouble)
        assertEquals(block.get(j), j.toDouble)
      }
    }
  }

  test("compressed array: small") {
    val block = CompressedArrayBlock(0L, 2)
    block.update(0, 1.0)
    assertEquals(block.get(0), 1.0)
    assert(block.get(1).isNaN)
  }

  test("compressed array: equals") {
    EqualsVerifier
      .forClass(classOf[CompressedArrayBlock])
      .suppress(Warning.NONFINAL_FIELDS)
      .verify()
  }
}
