/*
 * Copyright 2014-2017 Netflix, Inc.
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


import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import com.netflix.atlas.core.model._
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.spectator.api.Spectator


object BlockStats {
  private val registry = Spectator.globalRegistry
  private val arrayCount = registry.gauge("atlas.block.arrayCount", new AtomicLong(0L))
  private val constantCount = registry.gauge("atlas.block.constantCount", new AtomicLong(0L))
  private val sparseCount = registry.gauge("atlas.block.sparseCount", new AtomicLong(0L))
  private val arrayBytes = registry.gauge("atlas.block.arrayBytes", new AtomicLong(0L))
  private val constantBytes = registry.gauge("atlas.block.constantBytes", new AtomicLong(0L))
  private val sparseBytes = registry.gauge("atlas.block.sparseBytes", new AtomicLong(0L))

  private def inc(block: Block, count: AtomicLong, bytes: AtomicLong): Unit = {
    count.incrementAndGet
    bytes.addAndGet(block.byteCount)
  }

  def inc(block: Block): Unit = {
    block match {
      case b: ArrayBlock    => inc(block, arrayCount, arrayBytes)
      case b: ConstantBlock => inc(block, constantCount, constantBytes)
      case b: SparseBlock   => inc(block, sparseCount, sparseBytes)
      case _ =>
    }
  }

  private def dec(block: Block, count: AtomicLong, bytes: AtomicLong): Unit = {
    count.decrementAndGet
    bytes.addAndGet(-block.byteCount)
  }

  def dec(block: Block): Unit = {
    block match {
      case b: ArrayBlock    => dec(block, arrayCount, arrayBytes)
      case b: ConstantBlock => dec(block, constantCount, constantBytes)
      case b: SparseBlock   => dec(block, sparseCount, sparseBytes)
      case _ =>
    }
  }

  def update(oldBlock: Block, newBlock: Block): Unit = {
    dec(oldBlock)
    inc(newBlock)
  }

  private def percent(value: Long, total: Long): Double = {
    if (total == 0) 0.0 else (100.0 * value / total)
  }

  private def mkStatMap(array: Long, constant: Long, sparse: Long): Any = {
    val raw = Map(
      "array" -> array,
      "constant" -> constant,
      "sparse" -> sparse)
    val total = raw.values.sum
    Map(
      "total" -> total,
      "raw" -> raw,
      "percents" -> raw.map(t => t._1 -> percent(t._2, total)))
  }

  def statsMap: Map[String, Any] = {
    Map(
      "counts" -> mkStatMap(arrayCount.get, constantCount.get, sparseCount.get),
      "bytes" -> mkStatMap(arrayBytes.get, constantBytes.get, sparseBytes.get))
  }
}

trait BlockStore {
  /** Returns true if this block store has data. */
  def hasData: Boolean

  /** Removes all blocks where the start time is before the specified cutoff. */
  def cleanup(cutoff: Long)

  def update(timestamp: Long, value: Double)

  def blockList: List[Block]

  def fetch(start: Long, end: Long, aggr: Int): Array[Double]

  def get(start: Long): Option[Block]
}

object MemoryBlockStore {
  private val freeArrayBlocks = new ArrayBlockingQueue[ArrayBlock](100000)

  def freeArrayBlock(b: ArrayBlock): Unit = {
    freeArrayBlocks.offer(b)
  }

  def newArrayBlock(start: Long, size: Int): ArrayBlock = {
    val b = freeArrayBlocks.poll()
    if (b == null || b.size != size) {
      ArrayBlock(start, size)
    } else {
      b.reset(start)
      b
    }
  }
}

class MemoryBlockStore(step: Long, blockSize: Int, numBlocks: Int) extends BlockStore {

  import com.netflix.atlas.core.db.MemoryBlockStore._

  private val blockStep = step * blockSize

  private[db] val blocks: Array[Block] = new Array[Block](numBlocks)

  private[db] var currentPos: Int = 0

  private[db] var currentBlock: ArrayBlock = null

  private def next(pos: Int): Int = {
    (pos + 1) % numBlocks
  }

  private def alignStart(start: Long): Long = {
    start - start % blockStep
  }

  private def newBlock(start: Long): Unit = {
    require(start % blockStep == 0, "start time " + start + " is not on block boundary")
    val oldBlock = Block.compress(currentBlock)
    val newBlock =
      if (oldBlock eq currentBlock) {
        newArrayBlock(start, blockSize)
      } else {
        currentBlock.reset(start)
        currentBlock
      }
    BlockStats.update(currentBlock, oldBlock)
    BlockStats.inc(newBlock)
    blocks(currentPos) = oldBlock
    currentBlock = newBlock
    currentPos = next(currentPos)
    if (blocks(currentPos) != null) {
      BlockStats.dec(blocks(currentPos))
    }
    blocks(currentPos) = currentBlock
  }

  def hasData: Boolean = (currentBlock != null)

  def cleanup(cutoff: Long): Unit = {
    var pos = 0
    while (pos < numBlocks) {
      val block = blocks(pos)
      if (block != null && block.start < cutoff) {
        blocks(pos) = null
        BlockStats.dec(block)
        if (block eq currentBlock) currentBlock = null
        if (block.isInstanceOf[ArrayBlock]) {
          freeArrayBlock(block.asInstanceOf[ArrayBlock])
        }
      }
      pos += 1
    }
  }

  def update(timestamp: Long, value: Double): Unit = {
    if (currentBlock == null) {
      currentBlock = newArrayBlock(alignStart(timestamp), blockSize)
      currentPos = next(currentPos)
      blocks(currentPos) = currentBlock
      BlockStats.inc(currentBlock)
    }
    var pos = ((timestamp - currentBlock.start) / step).asInstanceOf[Int]
    require(pos >= 0, "data is too old")
    if (pos >= blockSize) {
      newBlock(alignStart(timestamp))
      pos = ((timestamp - currentBlock.start) / step).asInstanceOf[Int]
    }
    currentBlock.buffer(pos) = value
  }

  def update(start: Long, values: List[Double]): Unit = {
    var t = start
    values.foreach { v =>
      update(t, v)
      t += step
    }
  }

  private def fill(blk: Block, buf: Array[Double], start: Long, end: Long, aggr: Int): Unit = {
    val s = start / step
    val e = end / step
    val bs = blk.start
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
    bs.result
  }

  def fetch(start: Long, end: Long, aggr: Int): Array[Double] = {
    val size = (end / step - start / step).asInstanceOf[Int] + 1
    val buffer = ArrayHelper.fill(size, Double.NaN)
    var pos = 0
    while (pos < numBlocks) {
      if (blocks(pos) != null) fill(blocks(pos), buffer, start, end, aggr)
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
    val buf = new StringBuilder
    (0 until numBlocks).foreach { i =>
      buf.append(i.toString).append(" => ").append(blocks(i))
      if (i == currentPos) buf.append(" (current)")
      buf.append("\n")
    }
    buf.toString
  }
}
