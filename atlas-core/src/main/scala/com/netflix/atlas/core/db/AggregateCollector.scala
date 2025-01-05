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

import com.netflix.atlas.core.model.Block
import com.netflix.atlas.core.model.CollectorStats
import com.netflix.atlas.core.model.CollectorStatsBuilder
import com.netflix.atlas.core.model.ConsolidationFunction
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.util.Math

object AggregateCollector {

  def apply(expr: DataExpr): AggregateCollector = expr match {
    case by: DataExpr.GroupBy          => new GroupByAggregateCollector(by)
    case _: DataExpr.All               => new AllAggregateCollector
    case _: DataExpr.Sum               => new SumAggregateCollector
    case _: DataExpr.Count             => new SumAggregateCollector
    case _: DataExpr.Min               => new MinAggregateCollector
    case _: DataExpr.Max               => new MaxAggregateCollector
    case DataExpr.Consolidation(af, _) => apply(af)
  }
}

/**
  * Collector for computing an aggregate or set of aggregates from a set of metric buffers.
  */
trait AggregateCollector {

  /** Add `b` to the aggregate. */
  def add(b: TimeSeriesBuffer): Unit

  /**
    * Add a block to the aggregate directly. The underlying buffer must be using the same step size
    * as the block storage.
    */
  def add(
    tags: Map[String, String],
    blocks: List[Block],
    aggr: Int,
    cf: ConsolidationFunction,
    multiple: Int,
    newBuffer: Map[String, String] => TimeSeriesBuffer
  ): Int

  /** Combine the data with another collector. */
  def combine(collector: AggregateCollector): Unit

  /** Returns the final set of aggregate buffers. */
  def result: List[TimeSeriesBuffer]

  /** Statistics for this collector. */
  def stats: CollectorStats

  def newCollector(af: DataExpr.AggregateFunction): AggregateCollector = {
    AggregateCollector(af)
  }
}

abstract class SimpleAggregateCollector extends AggregateCollector {

  import java.util.BitSet as JBitSet

  var buffer: TimeSeriesBuffer = null.asInstanceOf[TimeSeriesBuffer]
  var valueMask: JBitSet = null.asInstanceOf[JBitSet]
  var valueMultiple: Int = -1
  var valueCount: Int = 0

  val statBuffer = new CollectorStatsBuilder

  protected def aggregate(b1: TimeSeriesBuffer, b2: TimeSeriesBuffer): Unit

  def add(b: TimeSeriesBuffer): Unit = {
    statBuffer.updateInput(b.values.length)
    if (!b.isAllNaN) {
      valueCount = 1
      if (buffer == null) {
        statBuffer.updateOutput(b.values.length)
        buffer = b.copy
      } else {
        aggregate(buffer, b)
      }
    }
  }

  def add(
    tags: Map[String, String],
    blocks: List[Block],
    aggr: Int,
    cf: ConsolidationFunction,
    multiple: Int,
    newBuffer: Map[String, String] => TimeSeriesBuffer
  ): Int = {
    if (buffer == null) {
      buffer = newBuffer(tags)
      statBuffer.updateOutput(buffer.values.length)
      if (cf == ConsolidationFunction.Avg && multiple > 1) {
        valueMask = new JBitSet(buffer.values.length * multiple)
        valueMultiple = multiple
      }
    }
    statBuffer.updateInput(blocks)

    val op = aggr match {
      case Block.Sum   => Math.addNaN _
      case Block.Count => Math.addNaN _
      case Block.Min   => Math.minNaN _
      case Block.Max   => Math.maxNaN _
    }

    blocks.foreach { b =>
      if (valueMask != null) {
        val v = buffer.aggrBlock(b, aggr, ConsolidationFunction.Sum, multiple, op)
        buffer.valueMask(valueMask, b, multiple)
        valueCount += v
      } else {
        val v = buffer.aggrBlock(b, aggr, cf, multiple, op)
        valueCount += v
      }
    }
    buffer.values.length
  }

  def combine(collector: AggregateCollector): Unit = {
    collector match {
      case c: SimpleAggregateCollector =>
        if (buffer == null && c.buffer != null) {
          buffer = c.buffer
          if (!buffer.isAllNaN) valueCount = 1
          statBuffer.update(c.stats)
        } else if (c.buffer != null) {
          aggregate(buffer, c.buffer)
          // 0 out output, since that is only based on the result of a single collector
          val tmp = c.stats.copy(outputLines = 0, outputDatapoints = 0)
          statBuffer.update(tmp)
        }
      case _ =>
        throw new IllegalStateException("cannot combine collectors of different types")
    }
  }

  def result: List[TimeSeriesBuffer] = {
    if (valueMask != null) {
      buffer.average(valueMask, valueMultiple)
      valueMask = null
    }
    if (buffer == null || valueCount == 0) Nil else List(buffer)
  }

  def stats: CollectorStats = statBuffer.result
}

/** Collector that returns a single buffer representing the sum of all individual buffers. */
class SumAggregateCollector extends SimpleAggregateCollector {

  protected def aggregate(b1: TimeSeriesBuffer, b2: TimeSeriesBuffer): Unit = b1.add(b2)
}

/** Collector that returns a single buffer representing the min of all individual buffers. */
class MinAggregateCollector extends SimpleAggregateCollector {

  protected def aggregate(b1: TimeSeriesBuffer, b2: TimeSeriesBuffer): Unit = b1.min(b2)
}

/** Collector that returns a single buffer representing the max of all individual buffers. */
class MaxAggregateCollector extends SimpleAggregateCollector {

  protected def aggregate(b1: TimeSeriesBuffer, b2: TimeSeriesBuffer): Unit = b1.max(b2)
}

abstract class LimitedAggregateCollector extends AggregateCollector {

  protected def checkLimits(numLines: Int, numDatapoints: Int): Unit = {
    check("lines", numLines, Limits.maxLines)
    check("datapoints", numDatapoints, Limits.maxDatapoints)
  }

  private def check(what: String, actual: Int, limit: Int): Unit = {
    require(actual <= limit, s"too many $what: $actual > $limit")
  }
}

class GroupByAggregateCollector(ft: DataExpr.GroupBy) extends LimitedAggregateCollector {

  type KeyType = String
  private val buffers = collection.mutable.HashMap.empty[KeyType, AggregateCollector]
  private var bufferSize = -1

  def add(b: TimeSeriesBuffer): Unit = {

    // Create key and exit early on failure
    val k = ft.keyString(b.tags)
    if (k == null) return

    // Add the data to the existing collector for the key or create a new one
    val c = buffers.getOrElse(
      k, {
        checkLimits(buffers.size + 1, (buffers.size + 1) * b.values.length)
        val collector = newCollector(ft.af)
        buffers += (k -> collector)
        collector
      }
    )
    c.add(b)
  }

  def add(
    tags: Map[String, String],
    blocks: List[Block],
    aggr: Int,
    cf: ConsolidationFunction,
    multiple: Int,
    newBuffer: Map[String, String] => TimeSeriesBuffer
  ): Int = {

    // Create key and exit early on failure
    val k = ft.keyString(tags)
    if (k == null) return 0

    // Add the data to the existing collector for the key or create a new one
    val c = buffers.getOrElse(
      k, {
        if (bufferSize > 0) {
          checkLimits(buffers.size + 1, (buffers.size + 1) * bufferSize)
        }
        val collector = newCollector(ft.af)
        buffers += (k -> collector)
        collector
      }
    )
    bufferSize = c.add(tags, blocks, aggr, cf, multiple, newBuffer)
    bufferSize
  }

  def combine(collector: AggregateCollector): Unit = {
    throw new UnsupportedOperationException("combine is only supported for simple aggregations")
  }

  def result: List[TimeSeriesBuffer] = buffers.values.flatMap(_.result).toList

  def stats: CollectorStats = {
    val statBuffer = new CollectorStatsBuilder
    buffers.values.foreach(c => statBuffer.update(c.stats))
    statBuffer.result
  }
}

class AllAggregateCollector extends LimitedAggregateCollector {

  private val builder = List.newBuilder[TimeSeriesBuffer]
  private var numLines = 0
  val statBuffer = new CollectorStatsBuilder

  def add(b: TimeSeriesBuffer): Unit = {
    numLines += 1
    checkLimits(numLines, numLines * b.values.length)
    statBuffer.updateInput(b.values.length)
    statBuffer.updateOutput(b.values.length)
    builder += b.copy
  }

  def add(
    tags: Map[String, String],
    blocks: List[Block],
    aggr: Int,
    cf: ConsolidationFunction,
    multiple: Int,
    newBuffer: Map[String, String] => TimeSeriesBuffer
  ): Int = {
    var valueCount = 0
    val buffer = newBuffer(tags)

    val op = aggr match {
      case Block.Sum   => Math.addNaN _
      case Block.Count => Math.addNaN _
      case Block.Min   => Math.minNaN _
      case Block.Max   => Math.maxNaN _
    }

    blocks.foreach { b =>
      val v = buffer.aggrBlock(b, aggr, cf, multiple, op)
      valueCount += v
    }

    if (valueCount > 0) add(buffer)
    buffer.values.length
  }

  def combine(collector: AggregateCollector): Unit = {
    throw new UnsupportedOperationException("combine is only supported for simple aggregations")
  }

  def result: List[TimeSeriesBuffer] = builder.result()

  def stats: CollectorStats = statBuffer.result
}
