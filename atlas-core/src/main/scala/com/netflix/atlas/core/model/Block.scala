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
package com.netflix.atlas.core.model

import java.util

//import com.netflix.atlas.core.block.DefaultBlockCodec
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.Math
import gnu.trove.map.hash.TDoubleIntHashMap
import gnu.trove.procedure.TDoubleIntProcedure


/**
 * Helper functions for working with blocks.
 */
object Block {

  // Aggregate types
  final val Sum = 0
  final val Count = 1
  final val Min = 2
  final val Max = 3

  // Max size for an array block. The current value is set to ensure we don't overflow the byte
  // indexes used for SparseBlock when compressing and block sizes are typically for one minute or
  // one hour periods.
  val MAX_SIZE = 120

  /**
   * Attempts to compress an array block into a more compact type. If compression fails to provide
   * savings the orignal block will be returned.
   */
  def compress(block: ArrayBlock): Block = {
    if (block.size < 10) {
      block
    } else {
      // Keeps track of values we have already seen and maps them to an index. NaN doesn't equal
      // anything so it cannot be found in the map. We special case it to always have index -1.
      val idxMap = new TDoubleIntHashMap(10, 0.5f, Double.NaN, SparseBlock.NOT_FOUND)
      var nextIdx = 0

      // Index into the value array
      val indexes = new Array[Byte](block.size)

      // Populate index array and value map from the array block
      var isConstant = true
      var prev = 0
      var i = 0
      while (i < block.size) {
        val v = block.buffer(i)
        val predefIdx = SparseBlock.predefinedIndex(v)
        var idx = if (predefIdx == SparseBlock.UNDEFINED) idxMap.get(v) else predefIdx
        if (idx == SparseBlock.NOT_FOUND) {
          if (nextIdx == MAX_SIZE) return block
          idx = nextIdx
          nextIdx += 1
          idxMap.put(v, idx)
        }
        indexes(i) = idx.asInstanceOf[Byte]
        if (i > 0) isConstant &&= (prev == idx)
        prev = idx.asInstanceOf[Byte]
        i += 1
      }

      // Populate values array
      val values = new Array[Double](idxMap.size)
      idxMap.forEachEntry(new FillArrayProcedure(values))

      // Choose the appropriate block type
      if (isConstant)
        ConstantBlock(block.start, block.size, SparseBlock.get(indexes(0), values))
      else if (idxMap.size < block.size / 2)
        SparseBlock(block.start, indexes, values)
      else
        block
    }
  }

  /**
   * Attempts to compress an array block into a more compact type including options that may lead
   * to some loss of precision.
   */
  def lossyCompress(block: ArrayBlock): Block = {
    compress(block) match {
      case b: ArrayBlock => FloatArrayBlock(b)
      case b: Block      => b
    }
  }

  /**
   * Compresses a block if it is not already a compressed type.
   */
  def compressIfNeeded(block: Block): Block = {
    block match {
      case b: ArrayBlock  => lossyCompress(b)
      case b: RollupBlock => b.compress
      case b: Block       => b
    }
  }

  /** Add the data from two blocks. */
  def add(block1: Block, block2: Block): Block = {
    require(block1.size == block2.size, "block sizes: %d != %d".format(block1.size, block2.size))
    (block1, block2) match {
      case (b1: ArrayBlock, _) => b1.add(block2); b1
      case (_, b2: ArrayBlock) => b2.add(block1); b2
      case _ =>
        val b1 = block1.toArrayBlock
        b1.add(block2)
        b1
    }
  }

  /**
   * Merge the data from two blocks. The general policy is that a value is preferred over NaN. If
   * there are different values for a given time the larger value will be selected.
   */
  def merge(block1: Block, block2: Block): Block = {
    require(block1.size == block2.size, "block sizes: %d != %d".format(block1.size, block2.size))
    val result = (block1, block2) match {
      case (b1: ArrayBlock, _) => b1.merge(block2); b1
      case (_, b2: ArrayBlock) => b2.merge(block1); b2
      case _ =>
        val b1 = block1.toArrayBlock
        b1.merge(block2)
        b1
    }
    compress(result)
  }

  /** Returns a byte array representing the block data. The start time will not be stored. */
  /*def toByteArray(block: Block): Array[Byte] = {
    val buffer = ByteBuffer.allocate(block.byteCount)
    val codec = new DefaultBlockCodec(block.start)
    codec.encode(buffer, block)
    buffer.array
  }*/

  /** Returns a block decoded from the buffer created using `toByteArray`. */
  /*def fromByteArray(start: Long, bytes: Array[Byte]): Block = {
    val buffer = ByteBuffer.wrap(bytes)
    val codec = new DefaultBlockCodec(start)
    codec.decode(buffer)
  }*/

  /** Fills an array based on a map of double to int. */
  private class FillArrayProcedure(values: Array[Double]) extends TDoubleIntProcedure {
    def execute(v: Double, idx: Int): Boolean = {
      if (idx >= 0) values(idx) = v
      true
    }
  }
}

/**
 * Represents a fixed size window of metric data. All block implementations provide fast
 * random access to data.
 */
sealed trait Block {
  /** Start time for the block (epoch in milliseconds). */
  def start: Long

  /** Number of data points to store in the block. */
  def size: Int

  /**
   * Return the value for a given position in the block. All implementations should make this
   * a constant time operation. The default implementation assumes a single value.
   *
   * @param pos   position to read, value should be in the interval [0,size).
   * @param aggr  the aggregate value to read from the block
   */
  def get(pos: Int, aggr: Int = Block.Sum): Double = {
    import java.lang.{Double => JDouble}
    val v = get(pos)
    (aggr: @scala.annotation.switch) match {
      case Block.Sum   => v
      case Block.Count => if (JDouble.isNaN(v)) Double.NaN else 1.0
      case Block.Min   => v
      case Block.Max   => v
    }
  }

  /**
   * Return the value for a given position in the block. All implementations should make this
   * a constant time operation.
   *
   * @param pos   position to read, value should be in the interval [0,size).
   */
  def get(pos: Int): Double

  /** Number of bytes required to store this block in a simple binary representation. */
  def byteCount: Int

  /** Returns a copy of the block as a simple array-backed block. */
  def toArrayBlock: ArrayBlock = {
    val b = ArrayBlock(start, size)
    var i = 0
    while (i < size) {
      b.buffer(i) = get(i)
      i += 1
    }
    b
  }

  /**
   * Used to get a quick estimate of the size of numeric primatives and arrays of numeric
   * primitives.
   */
  def sizeOf(value: Any): Int = value match {
    case v: Boolean         => 1
    case v: Byte            => 1
    case v: Short           => 2
    case v: Int             => 4
    case v: Long            => 8
    case v: Float           => 4
    case v: Double          => 8

    // Assume integer length + size for each value
    case vs: Array[Boolean] => 4 + vs.length
    case vs: Array[Byte]    => 4 + vs.length
    case vs: Array[Short]   => 4 + 2 * vs.length
    case vs: Array[Int]     => 4 + 4 * vs.length
    case vs: Array[Long]    => 4 + 8 * vs.length
    case vs: Array[Float]   => 4 + 4 * vs.length
    case vs: Array[Double]  => 4 + 8 * vs.length
  }
}

/**
 * Block that stores the raw data in an array.
 *
 * @param start  start time for the block (epoch in milliseconds)
 * @param size   number of data points to store in the block
 */
case class ArrayBlock(var start: Long, size: Int) extends Block {

  val buffer = ArrayHelper.fill(size, Double.NaN)

  def get(pos: Int): Double = buffer(pos)
  val byteCount: Int = 2 + sizeOf(buffer)

  override def toArrayBlock: ArrayBlock = this

  /** Reset this block so it can be re-used. */
  def reset(t: Long) {
    start = t
    util.Arrays.fill(buffer, Double.NaN)
  }

  /** Add contents of another block to this block. */
  def add(b: Block, aggr: Int = Block.Sum) {
    var i = 0
    while (i < size) {
      buffer(i) = Math.addNaN(buffer(i), b.get(i, aggr))
      i += 1
    }
  }

  /** Select the minimum value of this block or `b`. */
  def min(b: Block, aggr: Int = Block.Min) {
    var i = 0
    while (i < size) {
      buffer(i) = Math.minNaN(buffer(i), b.get(i, aggr))
      i += 1
    }
  }

  /** Select the maximum value of this block or `b`. */
  def max(b: Block, aggr: Int = Block.Max) {
    var i = 0
    while (i < size) {
      buffer(i) = Math.maxNaN(buffer(i), b.get(i, aggr))
      i += 1
    }
  }

  /**
   * Merge the data in block `b` with the data in this block. The merge will happen in-place and
   * the data for this block will be changed. The policy is:
   *
   * - A value is preferred over NaN
   *
   * - If both blocks have values the larger value is selected. This is somewhat arbitrary, but
   *   generally works for us as most data is positive and missing data due to failures somewhere
   *   lead to a smaller number.
   *
   * @return  number of values that were changed as a result of the merge operation
   */
  def merge(b: Block): Int = {
    import java.lang.{Double => JDouble}
    var changed = 0
    var i = 0
    while (i < size) {
      val v1 = buffer(i)
      val v2 = b.get(i)
      if (JDouble.isNaN(v1)) {
        buffer(i) = v2
        if (!JDouble.isNaN(v2)) changed += 1
      } else if (v1 < v2) {
        buffer(i) = v2
        changed += 1
      }
      i += 1
    }
    changed
  }

  override def equals(other: Any): Boolean = {
    // Follows guidelines from: http://www.artima.com/pins1ed/object-equality.html#28.4
    other match {
      case that: ArrayBlock =>
        that.canEqual(this) && java.util.Arrays.equals(buffer, that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = {
    val prime = 31
    var hc = prime
    hc = hc * prime + java.lang.Long.valueOf(start).hashCode()
    hc = hc * prime + java.util.Arrays.hashCode(buffer)
    hc
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[ArrayBlock]
  }

  override def toString: String = {
    val buf = new StringBuilder
    buf.append("ArrayBlock(").append(start).append(",").append(size).append(",")
    buf.append("Array(").append(buffer.mkString(",")).append(")")
    buf.append(")")
    buf.toString
  }
}

object FloatArrayBlock {
  def apply(b: ArrayBlock): FloatArrayBlock = {
    val fb = FloatArrayBlock(b.start, b.size)
    var i = 0
    while (i < b.size) {
      fb.buffer(i) = b.buffer(i).asInstanceOf[Float]
      i += 1
    }
    fb
  }
}

/**
 * Block that stores the raw data in an array using single-precision floats rather than doubles
 * to store the values.
 *
 * @param start  start time for the block (epoch in milliseconds)
 * @param size   number of data points to store in the block
 */
case class FloatArrayBlock(start: Long, size: Int) extends Block {

  val buffer = ArrayHelper.fill(size, Float.NaN)

  def get(pos: Int): Double = buffer(pos).asInstanceOf[Double]
  val byteCount: Int = 2 + sizeOf(buffer)

  override def equals(other: Any): Boolean = {
    // Follows guidelines from: http://www.artima.com/pins1ed/object-equality.html#28.4
    other match {
      case that: FloatArrayBlock =>
        that.canEqual(this) && java.util.Arrays.equals(buffer, that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = {
    val prime = 31
    var hc = prime
    hc = hc * prime + java.lang.Long.valueOf(start).hashCode()
    hc = hc * prime + java.util.Arrays.hashCode(buffer)
    hc
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[FloatArrayBlock]
  }

  override def toString: String = {
    val buf = new StringBuilder
    buf.append("FloatArrayBlock(").append(start).append(",").append(size).append(",")
    buf.append("Array(").append(buffer.mkString(",")).append(")")
    buf.append(")")
    buf.toString
  }
}

/**
 * Simple block type where all data points have the same value.
 *
 * @param start  start time for the block (epoch in milliseconds)
 * @param size   number of data points to store in the block
 * @param value  value for the data points
 */
case class ConstantBlock(start: Long, size: Int, value: Double) extends Block {
  def get(pos: Int): Double = value
  def byteCount: Int = 2 + sizeOf(size) + sizeOf(value)
}

/**
 * Constants used for sparse blocks. Common values that are often repeated are special cased to
 * further reduce storage. In particular the values for NaN, 0, and 1 are not stored.
 */
object SparseBlock {
  final val NOT_FOUND = -4
  final val NaN = -3
  final val ZERO = -2
  final val ONE = -1
  final val UNDEFINED = 0

  /** Returns the predefined index for the value, or 0 if it is not predefined. */
  def predefinedIndex(value: Double): Int = {
    value match {
      case v if v != v   => NaN
      case v if v == 0.0 => ZERO
      case v if v == 1.0 => ONE
      case v             => UNDEFINED
    }
  }

  /** Returns the value for a  */
  def get(pos: Int, values: Array[Double]): Double = {
    (pos: @scala.annotation.switch) match {
      case SparseBlock.NaN  => Double.NaN
      case SparseBlock.ZERO => 0.0
      case SparseBlock.ONE  => 1.0
      case _                => values(pos)
    }
  }
}

/**
 * A block optimized for storing a small set of discrete values.
 *
 * @param start    start time for the block (epoch in milliseconds)
 * @param indexes  stores the index into the values array for each slot in the block
 * @param values   set of distinct values
 */
case class SparseBlock(start: Long, indexes: Array[Byte], values: Array[Double]) extends Block {
  require(indexes.size > 0, "indexes cannot be empty")

  def size: Int = indexes.length

  def get(pos: Int): Double = {
    val idx: Int = indexes(pos)
    SparseBlock.get(idx, values)
  }

  val byteCount: Int = 2 + sizeOf(indexes) + sizeOf(values)

  override def equals(other: Any): Boolean = {
    // Follows guidelines from: http://www.artima.com/pins1ed/object-equality.html#28.4
    other match {
      case that: SparseBlock =>
        that.canEqual(this) &&
          util.Arrays.equals(values, that.values) &&
          util.Arrays.equals(indexes, that.indexes)
      case _ => false
    }
  }

  override def hashCode: Int = {
    val prime = 31
    var hc = prime
    hc = hc * prime + java.lang.Long.valueOf(start).hashCode()
    hc = hc * prime + java.util.Arrays.hashCode(indexes)
    hc = hc * prime + java.util.Arrays.hashCode(values)
    hc
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[SparseBlock]
  }

  override def toString: String = {
    val buf = new StringBuilder
    buf.append("SparseBlock(").append(start).append(",")
    buf.append("Array(").append(indexes.mkString(",")).append("),")
    buf.append("Array(").append(values.mkString(",")).append(")")
    buf.append(")")
    buf.toString
  }
}

object RollupBlock {
  def empty(start: Long, size: Int): RollupBlock = {
    val sum = ArrayBlock(start, size)
    val count = ArrayBlock(start, size)
    val min = ArrayBlock(start, size)
    val max = ArrayBlock(start, size)
    RollupBlock(sum, count, min, max)
  }
}

/**
 * A block representing a set of aggregates computed by rolling up a metric.
 */
case class RollupBlock(sum: Block, count: Block, min: Block, max: Block) extends Block {

  require(List(count, min, max).forall(_.size == sum.size), "all blocks must have the same size")
  require(List(count, min, max).forall(_.start == sum.start), "all blocks must have the same start")

  def start: Long = sum.start

  def size: Int = sum.size

  def byteCount: Int = 2 + sum.byteCount + count.byteCount + min.byteCount + max.byteCount

  def get(pos: Int): Double = sum.get(pos)

  override def get(pos: Int, aggr: Int): Double = {
    (aggr: @scala.annotation.switch) match {
      case Block.Sum   => sum.get(pos)
      case Block.Count => count.get(pos)
      case Block.Min   => min.get(pos)
      case Block.Max   => max.get(pos)
    }
  }

  def rollup(block: Block) {
    require(sum.start == block.start, s"invalid start time: ${sum.start} != ${block.start}")
    require(sum.size == block.size, s"invalid size: ${sum.size} != ${block.size}")
    sum.asInstanceOf[ArrayBlock].add(block, Block.Sum)
    count.asInstanceOf[ArrayBlock].add(block, Block.Count)
    min.asInstanceOf[ArrayBlock].min(block, Block.Min)
    max.asInstanceOf[ArrayBlock].max(block, Block.Max)
  }

  def compress: RollupBlock = {
    val s = Block.compressIfNeeded(sum)
    val c = Block.compressIfNeeded(count)
    val mn = Block.compressIfNeeded(min)
    val mx = Block.compressIfNeeded(max)
    RollupBlock(s, c, mn, mx)
  }
}
