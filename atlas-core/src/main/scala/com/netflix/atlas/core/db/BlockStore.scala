/*
 * Copyright 2014-2020 Netflix, Inc.
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
import com.netflix.spectator.api.patterns.PolledMeter

object BlockStats {
  private val registry = Spectator.globalRegistry
  private[db] val arrayCount = createGauge("atlas.block.arrayCount")
  private[db] val constantCount = createGauge("atlas.block.constantCount")
  private[db] val sparseCount = createGauge("atlas.block.sparseCount")
  private[db] val arrayBytes = createGauge("atlas.block.arrayBytes")
  private[db] val constantBytes = createGauge("atlas.block.constantBytes")
  private[db] val sparseBytes = createGauge("atlas.block.sparseBytes")

  private def createGauge(name: String): AtomicLong = {
    PolledMeter
      .using(registry)
      .withName(name)
      .monitorValue(new AtomicLong(0L))
  }

  private def inc(block: Block, count: AtomicLong, bytes: AtomicLong): Unit = {
    count.incrementAndGet
    bytes.addAndGet(block.byteCount)
  }

  def inc(block: Block): Unit = {
    block match {
      case b: ArrayBlock    => inc(block, arrayCount, arrayBytes)
      case b: ConstantBlock => inc(block, constantCount, constantBytes)
      case b: SparseBlock   => inc(block, sparseCount, sparseBytes)
      case b: RollupBlock   => b.blocks.foreach(inc)
      case _                =>
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
      case b: RollupBlock   => b.blocks.foreach(dec)
      case _                =>
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
    val raw = Map("array" -> array, "constant" -> constant, "sparse" -> sparse)
    val total = raw.values.sum
    Map("total" -> total, "raw" -> raw, "percents" -> raw.map(t => t._1 -> percent(t._2, total)))
  }

  def statsMap: Map[String, Any] = {
    Map(
      "counts" -> mkStatMap(arrayCount.get, constantCount.get, sparseCount.get),
      "bytes"  -> mkStatMap(arrayBytes.get, constantBytes.get, sparseBytes.get)
    )
  }

  def clear(): Unit = {
    arrayCount.set(0)
    sparseCount.set(0)
    constantCount.set(0)
    arrayBytes.set(0)
    sparseBytes.set(0)
    constantCount.set(0)
  }
}

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
  private val registry = Spectator.globalRegistry
  private val allocs = registry.counter("atlas.block.creationCount", "id", "alloc")
  private val reused = registry.counter("atlas.block.creationCount", "id", "reused")

  private val freeArrayBlocks = new ArrayBlockingQueue[ArrayBlock](100000)

  def freeArrayBlock(b: ArrayBlock): Unit = {
    freeArrayBlocks.offer(b)
  }

  def newArrayBlock(start: Long, size: Int): ArrayBlock = {
    val b = freeArrayBlocks.poll()
    if (b == null || b.size != size) {
      allocs.increment()
      ArrayBlock(start, size)
    } else {
      reused.increment()
      b.reset(start)
      b
    }
  }

  def freeRollupBlock(b: RollupBlock): Unit = {
    b.blocks.foreach {
      case a: ArrayBlock => freeArrayBlock(a)
      case _             =>
    }
  }

  def newRollupBlock(start: Long, size: Int): RollupBlock = {
    RollupBlock(
      newArrayBlock(start, size),
      newArrayBlock(start, size),
      newArrayBlock(start, size),
      newArrayBlock(start, size)
    )
  }
}

class MemoryBlockStore(step: Long, blockSize: Int, numBlocks: Int) extends BlockStore {

  import com.netflix.atlas.core.db.MemoryBlockStore._

  private val blockStep = step * blockSize

  private[db] val blocks: Array[Block] = new Array[Block](numBlocks)

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
    val oldBlock = currentBlock match {
      case b: ArrayBlock  => Block.compress(b)
      case b: RollupBlock => b.compress
      case b              => throw new MatchError(b)
    }
    val newBlock =
      if (oldBlock eq currentBlock) {
        // Unable to compress so the ArrayBlock cannot be reused
        if (rollup)
          newRollupBlock(start, blockSize)
        else
          newArrayBlock(start, blockSize)
      } else {
        // New compressed block created so we can reuse the ArrayBlock
        reused.increment()
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
    hasData = true
  }

  var hasData: Boolean = false

  def cleanup(cutoff: Long): Unit = {
    var pos = 0
    var nonEmpty = false
    while (pos < numBlocks) {
      val block = blocks(pos)
      if (block != null && block.start < cutoff) {
        blocks(pos) = null
        BlockStats.dec(block)
        if (block eq currentBlock) currentBlock = null
        block match {
          case b: ArrayBlock  => freeArrayBlock(b)
          case b: RollupBlock => freeRollupBlock(b)
          case _              =>
        }
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
      currentPos = next(currentPos)
      blocks(currentPos) = currentBlock
      BlockStats.inc(currentBlock)
      hasData = true
    }
    var pos = ((timestamp - currentBlock.start) / step).asInstanceOf[Int]
    require(pos >= 0, "data is too old")
    if (pos >= blockSize) {
      newBlock(alignStart(timestamp), rollup)
      pos = ((timestamp - currentBlock.start) / step).asInstanceOf[Int]
    }
    currentBlock.update(pos, value)
  }

  def update(timestamp: Long): Unit = {
    if (currentBlock != null && currentBlock.start == timestamp) {
      val oldBlock = currentBlock match {
        case b: ArrayBlock  => Block.compress(b)
        case b: RollupBlock => b.compress
        case b              => throw new MatchError(b)
      }
      BlockStats.update(currentBlock, oldBlock)
      blocks(currentPos) = oldBlock
      currentBlock = null
      currentPos = next(currentPos)
      if (blocks(currentPos) != null) {
        BlockStats.dec(blocks(currentPos))
      }
      blocks(currentPos) = currentBlock
    }
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
    bs.result()
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
