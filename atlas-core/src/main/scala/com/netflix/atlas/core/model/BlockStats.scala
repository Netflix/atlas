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

import com.netflix.spectator.api.Spectator
import com.netflix.spectator.api.Utils
import com.netflix.spectator.api.patterns.PolledMeter

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
  * Helper for tracking the number and amount of memory block data is using.
  */
object BlockStats {

  private val registry = Spectator.globalRegistry

  private val blocksCreated = registry.counter("atlas.blocksCreated")
  private val blocksDeleted = registry.counter("atlas.blocksDeleted")
  private val blocksResized = registry.counter("atlas.blocksResized")

  private val countGauges = new ConcurrentHashMap[String, AtomicLong]()

  private val byteGauges = new ConcurrentHashMap[String, AtomicLong]()

  private def getCountGauge(blockType: String): AtomicLong = {
    Utils.computeIfAbsent[String, AtomicLong](
      countGauges,
      blockType,
      k =>
        PolledMeter
          .using(registry)
          .withName("atlas.blockCount")
          .withTag("id", k)
          .monitorValue(new AtomicLong(0L))
    )
  }

  private def getBytesGauge(blockType: String): AtomicLong = {
    Utils.computeIfAbsent[String, AtomicLong](
      byteGauges,
      blockType,
      k =>
        PolledMeter
          .using(registry)
          .withName("atlas.blockBytes")
          .withTag("id", k)
          .monitorValue(new AtomicLong(0L))
    )
  }

  private def inc(block: Block, blockType: String): Unit = {
    getCountGauge(blockType).incrementAndGet()
    getBytesGauge(blockType).addAndGet(block.byteCount)
  }

  /** Increment stats when a new block is created. */
  def inc(block: Block): Unit = {
    blocksCreated.increment()
    block match {
      case _: ArrayBlock           => inc(block, "array")
      case _: FloatArrayBlock      => inc(block, "arrayFloat")
      case _: CompressedArrayBlock => inc(block, "arrayCompressed")
      case _: SparseBlock          => inc(block, "sparse")
      case _: ConstantBlock        => inc(block, "constant")
      case b: RollupBlock          => b.blocks.foreach(inc)
      case _                       =>
    }
  }

  private def dec(block: Block, blockType: String): Unit = {
    getCountGauge(blockType).decrementAndGet()
    getBytesGauge(blockType).addAndGet(-block.byteCount)
  }

  /** Decrement stats when a block is deleted and no longer being used. */
  def dec(block: Block): Unit = {
    blocksDeleted.increment()
    block match {
      case _: ArrayBlock           => dec(block, "array")
      case _: FloatArrayBlock      => dec(block, "arrayFloat")
      case _: CompressedArrayBlock => dec(block, "arrayCompressed")
      case _: SparseBlock          => dec(block, "sparse")
      case _: ConstantBlock        => dec(block, "constant")
      case b: RollupBlock          => b.blocks.foreach(dec)
      case _                       =>
    }
  }

  private def resize(blockType: String, before: Long, after: Long): Unit = {
    getBytesGauge(blockType).addAndGet(-before + after)
  }

  /** Decrement stats when a block is deleted and no longer being used. */
  def resize(block: Block, before: Long, after: Long): Unit = {
    blocksResized.increment()
    block match {
      case _: ArrayBlock           => resize("array", before, after)
      case _: FloatArrayBlock      => resize("arrayFloat", before, after)
      case _: CompressedArrayBlock => resize("arrayCompressed", before, after)
      case _: SparseBlock          => resize("sparse", before, after)
      case _: ConstantBlock        => resize("constant", before, after)
      case r: RollupBlock          => r.blocks.foreach(b => resize(b, before, after))
      case _                       =>
    }
  }

  def clear(): Unit = {
    countGauges.values().forEach(_.set(0L))
    byteGauges.values().forEach(_.set(0L))
  }

  def overallCount: Int = {
    countGauges.values().stream().mapToInt(_.get().toInt).sum()
  }

  def overallBytes: Long = {
    byteGauges.values().stream().mapToLong(_.get()).sum()
  }
}
