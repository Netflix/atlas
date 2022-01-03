/*
 * Copyright 2014-2022 Netflix, Inc.
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

import com.netflix.atlas.core.util.DoubleIntHashMap
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.Math

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
      //val idxMap = new TDoubleIntHashMap(10, 0.5f, Double.NaN, SparseBlock.NOT_FOUND)
      val idxMap = new DoubleIntHashMap
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
        var idx =
          if (predefIdx == SparseBlock.UNDEFINED) idxMap.get(v, SparseBlock.NOT_FOUND)
          else predefIdx
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
      idxMap.foreach { (k, v) =>
        values(v) = k
      }

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
    (block1, block2) match {
      case (b1: RollupBlock, b2: RollupBlock) => rollupMerge(b1, b2)
      case (b1: RollupBlock, _)               => b1.compress
      case (_, b2: RollupBlock)               => b2.compress
      case (b1: ArrayBlock, _)                => b1.merge(block2); compress(b1)
      case (_, b2: ArrayBlock)                => b2.merge(block1); compress(b2)
      case _                                  => arrayMerge(block1, block2)
    }
  }

  private def rollupMerge(b1: RollupBlock, b2: RollupBlock): Block = {
    val merged = RollupBlock(
      min = merge(b1.min, b2.min),
      max = merge(b1.max, b2.max),
      sum = merge(b1.sum, b2.sum),
      count = merge(b1.count, b2.count)
    )
    merged.compress
  }

  private def arrayMerge(block1: Block, block2: Block): Block = {
    val b1 = block1.toArrayBlock
    b1.merge(block2)
    compress(b1)
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
    case _: Boolean => 1
    case _: Byte    => 1
    case _: Short   => 2
    case _: Int     => 4
    case _: Long    => 8
    case _: Float   => 4
    case _: Double  => 8

    // Assume integer length + size for each value
    case vs: Array[Boolean] => 4 + vs.length
    case vs: Array[Byte]    => 4 + vs.length
    case vs: Array[Short]   => 4 + 2 * vs.length
    case vs: Array[Int]     => 4 + 4 * vs.length
    case vs: Array[Long]    => 4 + 8 * vs.length
    case vs: Array[Float]   => 4 + 4 * vs.length
    case vs: Array[Double]  => 4 + 8 * vs.length

    case v => throw new MatchError(v)
  }
}

/** Block type that can be update incrementally as data is coming in. */
trait MutableBlock extends Block {

  /** Update the value for the specified position. */
  def update(pos: Int, value: Double): Unit

  /** Reset this block so it can be re-used. */
  def reset(t: Long): Unit
}

/**
  * Block that stores the raw data in an array.
  *
  * @param start  start time for the block (epoch in milliseconds)
  * @param size   number of data points to store in the block
  */
case class ArrayBlock(var start: Long, size: Int) extends MutableBlock {

  val buffer: Array[Double] = ArrayHelper.fill(size, Double.NaN)

  def get(pos: Int): Double = buffer(pos)
  val byteCount: Int = 2 + sizeOf(buffer)

  override def toArrayBlock: ArrayBlock = this

  /** Update the value for the specified position. */
  def update(pos: Int, value: Double): Unit = buffer(pos) = value

  /** Reset this block so it can be re-used. */
  def reset(t: Long): Unit = {
    start = t
    util.Arrays.fill(buffer, Double.NaN)
  }

  /** Add contents of another block to this block. */
  def add(b: Block, aggr: Int = Block.Sum): Unit = {
    var i = 0
    while (i < size) {
      buffer(i) = Math.addNaN(buffer(i), b.get(i, aggr))
      i += 1
    }
  }

  /** Select the minimum value of this block or `b`. */
  def min(b: Block, aggr: Int = Block.Min): Unit = {
    var i = 0
    while (i < size) {
      buffer(i) = Math.minNaN(buffer(i), b.get(i, aggr))
      i += 1
    }
  }

  /** Select the maximum value of this block or `b`. */
  def max(b: Block, aggr: Int = Block.Max): Unit = {
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
    hc = hc * prime + java.lang.Long.hashCode(start)
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
    hc = hc * prime + java.lang.Long.hashCode(start)
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
  * Mutable block type that tries to compress data as it is updated. The main compression
  * benefit comes by not needed to store the most common values that are seen in the data.
  * Initially 2 bits per value will be used that indicate one of four common values: NaN,
  * 0, 1, 1/60. If another value is used, then it will update the buffer to use 4 bits per
  * value that can be one of the four common values or an index to an explicit value. If
  * more than 12 distinct values are seen, then it will update the buffer to just be an array
  * of double values.
  *
  * @param start
  *     Start time for the block.
  * @param size
  *     Number of datapoints to include in the block.
  */
final case class CompressedArrayBlock(start: Long, size: Int) extends MutableBlock {

  import CompressedArrayBlock._

  private var buffer = {
    if (size < 16)
      newArray(size, compressed = false)
    else
      newArray(ceilingDivide(size * 2, bitsPerLong), compressed = true)
  }

  private def newArray(n: Int, compressed: Boolean): Array[Long] = {
    val buf = new Array[Long](n)
    if (!compressed) {
      // Initialize to NaN if not compressed
      val nanBits = java.lang.Double.doubleToLongBits(Double.NaN)
      var i = 0
      while (i < n) {
        buf(i) = nanBits
        i += 1
      }
    }
    buf
  }

  private def determineMode(data: Array[Long]): Int = {
    if (size < 16) {
      Mode64Bit
    } else {
      val bitsPerValue = ceilingDivide(data.length * bitsPerLong, size)
      if (bitsPerValue < 4)
        Mode2Bit
      else if (bitsPerValue >= 64)
        Mode64Bit
      else
        Mode4Bit
    }
  }

  override def get(pos: Int): Double = {
    val data = buffer
    determineMode(data) match {
      case Mode2Bit => // 2 bits per value
        val v = get2(data(pos / bit2PerLong), pos % bit2PerLong)
        doubleValue(v)
      case Mode4Bit => // 4 bits per value
        val v = get4(data(pos / bit4PerLong), pos % bit4PerLong)
        if (v < 4)
          doubleValue(v)
        else
          java.lang.Double.longBitsToDouble(data(v - 4 + ceilingDivide(size * 4, bitsPerLong)))
      case Mode64Bit => // 64 bits per value
        java.lang.Double.longBitsToDouble(data(pos))
      case mode =>
        throw new IllegalStateException(s"unsupported mode: $mode")
    }
  }

  override def byteCount: Int = {
    byteCount(buffer)
  }

  private def byteCount(data: Array[Long]): Int = {
    2 + sizeOf(data)
  }

  override def update(pos: Int, value: Double): Unit = {
    determineMode(buffer) match {
      case Mode2Bit => // 2 bits per value
        val v = intValue(value)
        if (v == -1)
          switchToMode4Bit(pos, value)
        else {
          val i = pos / bit2PerLong
          buffer(i) = set2(buffer(i), pos % bit2PerLong, v)
        }
      case Mode4Bit => // 4 bits per value
        val v = intValue(value)
        if (v == -1)
          insertValue(pos, value)
        else {
          val i = pos / bit4PerLong
          buffer(i) = set4(buffer(i), pos % bit4PerLong, v)
        }
      case Mode64Bit => // 64 bits per value
        buffer(pos) = java.lang.Double.doubleToLongBits(value)
      case mode =>
        throw new IllegalStateException(s"unsupported mode: $mode")
    }
  }

  private def switchToMode4Bit(pos: Int, value: Double): Unit = {
    val baseSize = ceilingDivide(size * 4, bitsPerLong)
    val data = newArray(baseSize + 1, compressed = true)

    // Copy existing data
    val oldData = buffer
    var i = 0
    while (i < size) {
      val v = get2(oldData(i / bit2PerLong), i % bit2PerLong)
      val j = i / bit4PerLong
      data(j) = set4(data(j), i % bit4PerLong, v)
      i += 1
    }

    // Put in new value
    data(baseSize) = java.lang.Double.doubleToLongBits(value)
    val j = pos / bit4PerLong
    data(j) = set4(data(j), pos % bit4PerLong, 4)

    // Update buffer
    BlockStats.resize(this, byteCount(buffer), byteCount(data))
    buffer = data
  }

  private def switchToMode64Bit(pos: Int, value: Double): Unit = {
    val baseSize = ceilingDivide(size * 4, bitsPerLong)
    val data = newArray(size, compressed = false)

    // Copy existing data
    val oldData = buffer
    var i = 0
    while (i < size) {
      val v = get4(oldData(i / bit4PerLong), i % bit4PerLong)
      if (v < 4)
        data(i) = java.lang.Double.doubleToLongBits(doubleValue(v))
      else
        data(i) = oldData(v - 4 + baseSize)
      i += 1
    }

    // Put in new value
    data(pos) = java.lang.Double.doubleToLongBits(value)

    // Update buffer
    BlockStats.resize(this, byteCount(buffer), byteCount(data))
    buffer = data
  }

  private def insertValue(pos: Int, value: Double): Unit = {
    val v = java.lang.Double.doubleToLongBits(value)
    val baseSize = ceilingDivide(size * 4, bitsPerLong)
    var i = baseSize
    while (i < buffer.length) {
      if (buffer(i) == 0) {
        // No matches, put in value in empty spot
        buffer(i) = v
        val j = pos / bit4PerLong
        buffer(j) = set4(buffer(j), pos % bit4PerLong, i - baseSize + 4)
        return
      } else if (buffer(i) == v) {
        // Found a match, just update the position
        val j = pos / bit4PerLong
        buffer(j) = set4(buffer(j), pos % bit4PerLong, i - baseSize + 4)
        return
      }
      i += 1
    }

    // No matches found, grow or switch to 64 bit mode
    val numEntries = buffer.length - baseSize
    numEntries match {
      case n if n < 4 =>
        grow(baseSize + 4, baseSize, i, pos, v)
      case n if n < 12 =>
        grow(baseSize + 12, baseSize, i, pos, v)
      case _ =>
        switchToMode64Bit(pos, value)
    }
  }

  private def grow(n: Int, baseSize: Int, i: Int, pos: Int, v: Long): Unit = {
    val data = newArray(n, compressed = true)
    System.arraycopy(buffer, 0, data, 0, buffer.length)
    data(i) = v
    val j = pos / bit4PerLong
    data(j) = set4(data(j), pos % bit4PerLong, i - baseSize + 4)
    BlockStats.resize(this, byteCount(buffer), byteCount(data))
    buffer = data
  }

  override def reset(t: Long): Unit = {
    throw new UnsupportedOperationException("reset")
  }

  override def equals(other: Any): Boolean = {

    // Follows guidelines from: http://www.artima.com/pins1ed/object-equality.html#28.4
    other match {
      case that: CompressedArrayBlock =>
        that.canEqual(this) &&
        start == that.start &&
        size == that.size &&
        java.util.Arrays.equals(buffer, that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = {
    val prime = 31
    var hc = prime
    hc = hc * prime + java.lang.Long.hashCode(start)
    hc = hc * prime + java.lang.Integer.hashCode(size)
    hc = hc * prime + java.util.Arrays.hashCode(buffer)
    hc
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[CompressedArrayBlock]
  }
}

object CompressedArrayBlock {

  private val Mode2Bit = 0
  private val Mode4Bit = 1
  private val Mode64Bit = 2

  private val oneOver60 = 1.0 / 60.0

  private val bitsPerLong = 8 * 8

  private val bit2PerLong = bitsPerLong / 2

  private val bit4PerLong = bitsPerLong / 4

  private[core] def ceilingDivide(dividend: Int, divisor: Int): Int = {
    (dividend + divisor - 1) / divisor
  }

  private[core] def set2(buffer: Long, pos: Int, value: Long): Long = {
    val shift = pos * 2
    (buffer & ~(0x3L << shift)) | ((value & 0x3L) << shift)
  }

  private[core] def get2(buffer: Long, pos: Int): Int = {
    val shift = pos * 2
    ((buffer >>> shift) & 0x3L).asInstanceOf[Int]
  }

  private[core] def set4(buffer: Long, pos: Int, value: Long): Long = {
    val shift = pos * 4
    (buffer & ~(0xFL << shift)) | ((value & 0xFL) << shift)
  }

  private[core] def get4(buffer: Long, pos: Int): Int = {
    val shift = pos * 4
    ((buffer >>> shift) & 0xFL).asInstanceOf[Int]
  }

  private def intValue(value: Double): Int = {
    value match {
      case v if v != v         => 0
      case v if v == 0.0       => 1
      case v if v == 1.0       => 2
      case v if v == oneOver60 => 3
      case _                   => -1
    }
  }

  private def doubleValue(value: Int): Double = {
    value match {
      case 0 => Double.NaN
      case 1 => 0.0
      case 2 => 1.0
      case 3 => oneOver60
      case _ => Double.NaN
    }
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
  require(indexes.length > 0, "indexes cannot be empty")

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
    hc = hc * prime + java.lang.Long.hashCode(start)
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
case class RollupBlock(sum: Block, count: Block, min: Block, max: Block) extends MutableBlock {

  require(List(count, min, max).forall(_.size == sum.size), "all blocks must have the same size")
  require(
    List(count, min, max).forall(_.start == sum.start),
    "all blocks must have the same start"
  )

  def blocks: List[Block] = List(sum, count, min, max)

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

  def rollup(block: Block): Unit = {
    require(sum.start == block.start, s"invalid start time: ${sum.start} != ${block.start}")
    require(sum.size == block.size, s"invalid size: ${sum.size} != ${block.size}")
    sum.asInstanceOf[ArrayBlock].add(block, Block.Sum)
    count.asInstanceOf[ArrayBlock].add(block, Block.Count)
    min.asInstanceOf[ArrayBlock].min(block, Block.Min)
    max.asInstanceOf[ArrayBlock].max(block, Block.Max)
  }

  /** Update the value for the specified position. */
  def update(pos: Int, value: Double): Unit = {
    if (!value.isNaN) {
      updateSum(pos, value)
      updateCount(pos, value)
      updateMin(pos, value)
      updateMax(pos, value)
    }
  }

  private def updateSum(pos: Int, value: Double): Unit = {
    val block = sum.asInstanceOf[MutableBlock]
    block.update(pos, Math.addNaN(block.get(pos), value))
  }

  private def updateCount(pos: Int, value: Double): Unit = {
    val block = count.asInstanceOf[MutableBlock]
    block.update(pos, Math.addNaN(block.get(pos), 1.0))
  }

  private def updateMin(pos: Int, value: Double): Unit = {
    val block = min.asInstanceOf[MutableBlock]
    block.update(pos, Math.minNaN(block.get(pos), value))
  }

  private def updateMax(pos: Int, value: Double): Unit = {
    val block = max.asInstanceOf[MutableBlock]
    block.update(pos, Math.maxNaN(block.get(pos), value))
  }

  /** Reset this block so it can be re-used. */
  def reset(t: Long): Unit = {
    sum.asInstanceOf[ArrayBlock].reset(t)
    count.asInstanceOf[ArrayBlock].reset(t)
    min.asInstanceOf[ArrayBlock].reset(t)
    max.asInstanceOf[ArrayBlock].reset(t)
  }

  def compress: RollupBlock = {
    val s = Block.compressIfNeeded(sum)
    val c = Block.compressIfNeeded(count)
    val mn = Block.compressIfNeeded(min)
    val mx = Block.compressIfNeeded(max)
    RollupBlock(s, c, mn, mx)
  }
}
