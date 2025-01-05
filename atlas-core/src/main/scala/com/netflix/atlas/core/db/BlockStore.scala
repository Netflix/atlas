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

import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.util.ArrayHelper

trait BlockStore {

  /** Returns true if this block store has data. */
  def hasData: Boolean

  /** Removes all blocks where the start time is before the specified cutoff. */
  def cleanup(cutoff: Long): Unit

  /** Updates the store with a datapoint. */
  def update(timestamp: Long, value: Double, rollup: Boolean = false): Unit

  /**
    * Force and update with no data. This can be used to trigger the rotation and compression
    * of the currently updating block if no data is received after the block boundary. The
    * timestamp should be the start of the block to process.
    */
  def update(timestamp: Long): Unit

  def blockList: List[Block]

  def fetch(start: Long, end: Long, aggr: Int): Array[Double]

  def get(start: Long): Option[Block]
}

object MemoryBlockStore {

  def newArrayBlock(start: Long, size: Int): MutableBlock = {
    CompressedArrayBlock(start, size)
  }

  def newRollupBlock(start: Long, size: Int): RollupBlock = {
    RollupBlock(
      CompressedArrayBlock(start, size),
      CompressedArrayBlock(start, size),
      CompressedArrayBlock(start, size),
      CompressedArrayBlock(start, size)
    )
  }
}

class MemoryBlockStore(step: Long, blockSize: Int, numBlocks: Int) extends BlockStore {

  import com.netflix.atlas.core.db.MemoryBlockStore.*

  private val blockStep = step * blockSize

  private[db] val blocks: Array[MutableBlock] = new Array[MutableBlock](numBlocks)

  private[db] var currentPos: Int = 0

  private[db] var currentBlock: MutableBlock = _

  private def next(pos: Int): Int = {
    (pos + 1) % numBlocks
  }

  private def alignStart(start: Long): Long = {
    start - start % blockStep
  }

  private def newBlock(start: Long, rollup: Boolean): Unit = {
    require(start % blockStep == 0, s"start time $start is not on block boundary")
    val oldBlock = currentBlock
    val newBlock =
      if (rollup)
        newRollupBlock(start, blockSize)
      else
        newArrayBlock(start, blockSize)
    BlockStats.inc(newBlock)
    blocks(currentPos) = oldBlock
    currentBlock = newBlock
    currentPos = next(currentPos)
    if (blocks(currentPos) != null) BlockStats.dec(blocks(currentPos))
    blocks(currentPos) = currentBlock
    hasData = true
  }

  var hasData: Boolean = false

  def cleanup(cutoff: Long): Unit = {
    var pos = 0
    var nonEmpty = false
    while (pos < numBlocks) {
      val block = blocks(pos)
      if (block != null && block.start < cutoff) {
        if (blocks(pos) != null) BlockStats.dec(blocks(pos))
        blocks(pos) = null
        if (block eq currentBlock) currentBlock = null
      } else {
        nonEmpty = nonEmpty || (block != null)
      }
      pos += 1
    }
    hasData = nonEmpty
  }

  def update(timestamp: Long, value: Double, rollup: Boolean): Unit = {
    if (currentBlock == null) {
      currentBlock =
        if (rollup)
          newRollupBlock(alignStart(timestamp), blockSize)
        else
          newArrayBlock(alignStart(timestamp), blockSize)
      BlockStats.inc(currentBlock)
      currentPos = next(currentPos)
      if (blocks(currentPos) != null) BlockStats.dec(blocks(currentPos))
      blocks(currentPos) = currentBlock
      hasData = true
    }
    var pos = ((timestamp - currentBlock.start) / step).asInstanceOf[Int]
    if (pos >= blockSize) {
      // Exceeded window of current block, create a new one for the next
      // interval
      newBlock(alignStart(timestamp), rollup)
      pos = ((timestamp - currentBlock.start) / step).asInstanceOf[Int]
      currentBlock.update(pos, value)
    } else if (pos < 0) {
      // Out of order update received for an older block, try to update the
      // previous block
      val previousPos = (currentPos - 1) % numBlocks
      if (previousPos > 0 && blocks(previousPos) != null) {
        val previousBlock = blocks(previousPos)
        pos = ((timestamp - previousBlock.start) / step).asInstanceOf[Int]
        if (pos >= 0 && pos < blockSize) {
          previousBlock.update(pos, value)
        }
      }
    } else {
      currentBlock.update(pos, value)
    }
  }

  def update(timestamp: Long): Unit = {
    if (currentBlock != null && currentBlock.start == timestamp) {
      blocks(currentPos) = currentBlock
      currentBlock = null
    }
  }

  def update(start: Long, values: List[Double]): Unit = {
    var t = start
    values.foreach { v =>
      update(t, v)
      t += step
    }
  }

  private def fill(blk: Block, buf: Array[Double], start: Long, end: Long): Unit = {
    val s = start / step
    val e = end / step
    val bs = blk.start / step
    val be = bs + blockSize - 1
    if (e >= bs && s <= be) {
      val spos = if (s > bs) s else bs
      val epos = if (e < be) e else be
      var i = spos
      while (i <= epos) {
        buf((i - s).toInt) = blk.get((i - bs).toInt) // TODO: use proper aggregate
        i += 1
      }
    }
  }

  def blockList: List[Block] = {
    val bs = List.newBuilder[Block]
    var pos = 0
    while (pos < numBlocks) {
      val b = blocks(pos)
      if (b != null) bs += b
      pos += 1
    }
    bs.result()
  }

  def fetch(start: Long, end: Long, aggr: Int): Array[Double] = {
    val size = (end / step - start / step).asInstanceOf[Int] + 1
    val buffer = ArrayHelper.fill(size, Double.NaN)
    var pos = 0
    while (pos < numBlocks) {
      if (blocks(pos) != null) fill(blocks(pos), buffer, start, end)
      pos += 1
    }
    buffer
  }

  def get(start: Long): Option[Block] = {
    var pos = 0
    var block: Block = null
    while (block == null && pos < numBlocks) {
      val b = blocks(pos)
      if (b != null && b.start == start) block = b
      pos += 1
    }
    Option(block)
  }

  override def toString: String = {
    val buf = new java.lang.StringBuilder
    (0 until numBlocks).foreach { i =>
      buf.append(i.toString).append(" => ").append(blocks(i))
      if (i == currentPos) buf.append(" (current)")
      buf.append("\n")
    }
    buf.toString
  }
}
