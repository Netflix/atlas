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
package com.netflix.atlas.core.model

import org.scalatest.funsuite.AnyFunSuite

class BlockSuite extends AnyFunSuite {

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
    checkValues(b, (0 until 60).map(i => 42.0).toList)
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
      (0 until 5).map(i => 0) ++
      (5 until 37).map(i => SparseBlock.NaN) ++
      (37 until 60).map(i => 1)
    val indexes = data.map(_.asInstanceOf[Byte]).toArray
    val values = Array(42.0, 21.0)
    val b = SparseBlock(0L, indexes, values)
    val ab = b.toArrayBlock
    checkValues(b, ab.buffer.toList)
  }

  test("SparseBlock.get, size > 120") {
    val data =
      (0 until 5).map(i => 0) ++
      (5 until 37).map(i => SparseBlock.NaN) ++
      (37 until 360).map(i => 1)
    val indexes = data.map(_.asInstanceOf[Byte]).toArray
    val values = Array(42.0, 21.0)
    val b = SparseBlock(0L, indexes, values)
    val ab = b.toArrayBlock
    checkValues(b, ab.buffer.toList)
  }

  test("Block.get(pos, aggr)") {
    import java.lang.{Double => JDouble}
    val b = ArrayBlock(0L, 2)
    b.buffer(0) = 0.0
    b.buffer(1) = Double.NaN
    assert(b.get(0, Block.Sum) === 0.0)
    assert(b.get(0, Block.Count) === 1.0)
    assert(b.get(0, Block.Min) === 0.0)
    assert(b.get(0, Block.Max) === 0.0)
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
    //assert(nb.byteCount === 84)
    assert(nb.values.length === 2)
    (0 until 60).foreach { i =>
      assert(java.lang.Double.compare(b.get(i), nb.get(i)) === 0)
    }
  }

  test("compress, rle -> sparse, special values") {
    val data = List(5 -> 2.0, 37 -> Double.NaN, 59 -> 0.0)
    val b = rleBlock(0L, data)
    val nb = Block.compress(b.toArrayBlock).asInstanceOf[SparseBlock]
    assert(nb.values.length === 1)
    (0 until 60).foreach { i =>
      assert(java.lang.Double.compare(b.get(i), nb.get(i)) === 0)
    }
  }

  test("compress, rle -> sparse, large block") {
    val data = List(5 -> 2.0, 37 -> Double.NaN, 359 -> 0.0)
    val b = rleBlock(0L, data, 360)
    val nb = Block.compress(b.toArrayBlock).asInstanceOf[SparseBlock]
    assert(nb.values.length === 1)
    (0 until 360).foreach { i =>
      assert(java.lang.Double.compare(b.get(i), nb.get(i)) === 0)
    }
  }

  test("compress fails on large block") {
    val data = (0 until 360).map(i => i -> (if (i <= 129) i.toDouble else 42.42)).toList
    val b = rleBlock(0L, data, 360)
    val nb = Block.compress(b.toArrayBlock)
    (0 until 360).foreach { i =>
      assert(java.lang.Double.compare(b.get(i), nb.get(i)) === 0)
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
    assert(Block.lossyCompress(b) === FloatArrayBlock(b))
  }

  test("compress, small block") {
    val b = ArrayBlock(0L, 5)
    (0 until 5).foreach(i => b.buffer(i) = 42.0)
    assert(Block.compress(b) eq b)
  }

  test("merge") {
    val data1 = List(5    -> 42.0, 37 -> Double.NaN, 59 -> 21.0)
    val data2 = List(9    -> 41.0, 45 -> Double.NaN, 59 -> 22.0)
    val expected = List(5 -> 42.0, 9  -> 41.0, 37       -> Double.NaN, 45 -> 21.0, 59 -> 22.0)
    val b1 = rleBlock(0L, data1)
    val b2 = rleBlock(0L, data2)
    val b3 = Block.merge(b1, b2)
    val expBlock = rleBlock(0L, expected).toArrayBlock
    (0 until 60).foreach { i =>
      assert(java.lang.Double.compare(b3.get(i), expBlock.get(i)) === 0)
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
    assert(b3 === expected)
  }

  test("merge prefer rollup to scalar") {
    val b1 = RollupBlock(
      min = ConstantBlock(0L, 60, 1.0),
      max = ConstantBlock(0L, 60, 50.0),
      sum = ConstantBlock(0L, 60, 51.0),
      count = ConstantBlock(0L, 60, 2.0)
    )
    val b2 = ConstantBlock(0L, 60, 2.0)
    assert(b1 === Block.merge(b1, b2))
    assert(b1 === Block.merge(b2, b1))
  }

  test("rollup") {
    import java.lang.{Double => JDouble}

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
      assert(r.get(i, Block.Sum) === 1.0)
      assert(r.get(i, Block.Count) === 1.0)
      assert(r.get(i, Block.Min) === 1.0)
      assert(r.get(i, Block.Max) === 1.0)
    }

    r.rollup(ConstantBlock(0L, n, 3.0))
    (0 until n).foreach { i =>
      assert(r.get(i, Block.Sum) === 4.0)
      assert(r.get(i, Block.Count) === 2.0)
      assert(r.get(i, Block.Min) === 1.0)
      assert(r.get(i, Block.Max) === 3.0)
    }

    r.rollup(ConstantBlock(0L, n, 0.5))
    (0 until n).foreach { i =>
      assert(r.get(i, Block.Sum) === 4.5)
      assert(r.get(i, Block.Count) === 3.0)
      assert(r.get(i, Block.Min) === 0.5)
      assert(r.get(i, Block.Max) === 3.0)
    }
  }
}
