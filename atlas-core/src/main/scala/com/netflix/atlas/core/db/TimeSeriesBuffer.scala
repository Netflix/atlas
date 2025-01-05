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

import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.Block
import com.netflix.atlas.core.model.ConsolidationFunction
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.LazyTaggedItem
import com.netflix.atlas.core.model.MapStepTimeSeq
import com.netflix.atlas.core.model.TimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.Math

object TimeSeriesBuffer {

  def apply(
    tags: Map[String, String],
    step: Long,
    start: Long,
    vs: Array[Double]
  ): TimeSeriesBuffer = {
    new TimeSeriesBuffer(tags, new ArrayTimeSeq(DsType(tags), start / step * step, step, vs))
  }

  def apply(tags: Map[String, String], step: Long, start: Long, end: Long): TimeSeriesBuffer = {
    val s = start / step
    val e = end / step

    val size = (e - s).toInt + 1
    val buffer = ArrayHelper.fill(size, Double.NaN)
    new TimeSeriesBuffer(tags, new ArrayTimeSeq(DsType(tags), s * step, step, buffer))
  }

  def apply(
    tags: Map[String, String],
    step: Long,
    start: Long,
    end: Long,
    blocks: List[Block],
    aggr: Int
  ): TimeSeriesBuffer = {

    val s = start / step
    val e = end / step

    val size = (e - s).toInt + 1
    val buffer = ArrayHelper.fill(size, Double.NaN)
    blocks.foreach { block =>
      fill(block, buffer, step, s, e, aggr)
    }

    new TimeSeriesBuffer(tags, new ArrayTimeSeq(DsType(tags), start, step, buffer))
  }

  private def fill(
    blk: Block,
    buf: Array[Double],
    step: Long,
    s: Long,
    e: Long,
    aggr: Int
  ): Unit = {
    val bs = blk.start / step
    val be = bs + blk.size - 1
    if (e >= bs && s <= be) {
      val spos = if (s > bs) s else bs
      val epos = if (e < be) e else be
      var i = spos
      while (i <= epos) {
        buf((i - s).toInt) = blk.get((i - bs).toInt, aggr)
        i += 1
      }
    }
  }

  def sum(buffers: List[TimeSeriesBuffer]): Option[TimeSeriesBuffer] = {
    buffers match {
      case b :: bs =>
        bs.foreach(b.add)
        Some(b)
      case Nil => None
    }
  }

  def max(buffers: List[TimeSeriesBuffer]): Option[TimeSeriesBuffer] = {
    buffers match {
      case b :: bs =>
        bs.foreach(b.max)
        Some(b)
      case Nil => None
    }
  }

  def min(buffers: List[TimeSeriesBuffer]): Option[TimeSeriesBuffer] = {
    buffers match {
      case b :: bs =>
        bs.foreach(b.min)
        Some(b)
      case Nil => None
    }
  }

  def count(buffers: List[TimeSeriesBuffer]): Option[TimeSeriesBuffer] = {
    buffers match {
      case b :: bs =>
        b.initCount()
        bs.foreach(b.count)
        Some(b)
      case Nil => None
    }
  }
}

/**
  * Mutable buffer for efficiently manipulating metric data.
  */
final class TimeSeriesBuffer(val tags: Map[String, String], val data: ArrayTimeSeq)
    extends TimeSeries
    with TimeSeq
    with LazyTaggedItem {

  def label: String = TimeSeries.toLabel(tags)

  def dsType: DsType = data.dsType

  def step: Long = data.step

  def start: Long = data.start

  def values: Array[Double] = data.data

  def apply(t: Long): Double = data(t)

  import java.lang.Double as JDouble
  import java.util.BitSet as JBitSet

  /** Returns true if the buffer is all NaN values. */
  def isAllNaN: Boolean = {
    var hasValue = false
    var i = 0
    while (i < values.length && !hasValue) {
      hasValue = !JDouble.isNaN(values(i))
      i += 1
    }
    !hasValue
  }

  /**
    * Create a deep copy of this buffer.
    */
  def copy: TimeSeriesBuffer = {
    new TimeSeriesBuffer(tags, new ArrayTimeSeq(data.dsType, start, step, values.clone))
  }

  /** Aggregate the data from the block into this buffer. */
  private[db] def aggrBlock(
    block: Block,
    aggr: Int,
    cf: ConsolidationFunction,
    multiple: Int,
    op: (Double, Double) => Double
  ): Int = {
    val s = start / step
    val e = values.length + s - 1
    val bs = block.start / step
    val be = bs + block.size / multiple - 1
    var valueCount = 0
    if (e >= bs && s <= be) {
      val spos = if (s > bs) s else bs
      val epos = if (e < be) e else be
      var i = spos // Index to this buffer
      var j = (i - bs).toInt * multiple // Index into the block
      while (i <= epos) {
        val pos = (i - s).toInt
        val v = cf.compute(block, j, aggr, multiple)
        values(pos) = op(values(pos), v)
        if (!values(pos).isNaN) valueCount += 1
        i += 1
        j += multiple
      }
    }
    valueCount
  }

  /** Aggregate the data from the block into this buffer. */
  private[db] def valueMask(mask: JBitSet, block: Block, multiple: Int): Unit = {
    val blkStep = step / multiple
    val s = start / blkStep
    val e = values.length * multiple + s - 1
    val bs = block.start / blkStep
    val be = bs + block.size - 1
    if (e >= bs && s <= be) {
      val spos = if (s > bs) s else bs
      val epos = if (e < be) e else be
      var i = spos
      while (i <= epos) {
        val v = block.get((i - bs).toInt)
        if (!v.isNaN) mask.set((i - s).toInt)
        i += 1
      }
    }
  }

  private[db] def average(mask: JBitSet, multiple: Int): Unit = {
    if (data.dsType == DsType.Gauge) {
      // For gauges, only count values and ignore NaN when computing the denominator
      // of the average. Treating NaN as zero can lead to values that do not match
      // possible values for the gauge.
      val end = values.length * multiple
      var count = 0
      var i = 0
      while (i < end) {
        if (mask.get(i)) count += 1
        i += 1
        if (i % multiple == 0) {
          if (count > 0)
            values(i / multiple - 1) /= count
          else
            values(i / multiple - 1) = Double.NaN
          count = 0
        }
      }
    } else {
      // For rates, any ds type other than counter, treat NaN values as 0 for the
      // purposes of the consolidated average. This is consistent with a counter that
      // had no activity for a portion of the consolidated interval.
      val end = values.length
      var i = 0
      while (i < end) {
        values(i) /= multiple
        i += 1
      }
    }
  }

  /**
    * Add the corresponding positions of the two buffers. The buffers must have
    * the same settings.
    */
  def add(ts: TimeSeriesBuffer): Unit = {
    val nts = ts.normalize(step, start, values.length)
    val length = scala.math.min(values.length, nts.values.length)
    var pos = 0
    while (pos < length) {
      values(pos) = Math.addNaN(values(pos), nts.values(pos))
      pos += 1
    }
  }

  def add(block: Block): Int = {
    aggrBlock(block, Block.Sum, ConsolidationFunction.Sum, 1, Math.addNaN)
  }

  /**
    * Add a constant value to all positions of this buffer.
    */
  def add(v: Double): Unit = {
    var pos = 0
    while (pos < values.length) {
      values(pos) = Math.addNaN(values(pos), v)
      pos += 1
    }
  }

  /**
    * Subtract the corresponding positions of the two buffers. The buffers must
    * have the same settings. The tags for the new buffer will be the
    * intersection.
    */
  def subtract(ts: TimeSeriesBuffer): Unit = {
    val nts = ts.normalize(step, start, values.length)
    val length = scala.math.min(values.length, nts.values.length)
    var pos = 0
    while (pos < length) {
      values(pos) = values(pos) - nts.values(pos)
      pos += 1
    }
  }

  /**
    * Subtract a constant value to all positions of this buffer.
    */
  def subtract(v: Double): Unit = {
    var pos = 0
    while (pos < values.length) {
      values(pos) = values(pos) - v
      pos += 1
    }
  }

  /**
    * Multiply the corresponding positions of the two buffers. The buffers must
    * have the same settings. The tags for the new buffer will be the
    * intersection.
    */
  def multiply(ts: TimeSeriesBuffer): Unit = {
    val nts = ts.normalize(step, start, values.length)
    val length = scala.math.min(values.length, nts.values.length)
    var pos = 0
    while (pos < length) {
      values(pos) = values(pos) * nts.values(pos)
      pos += 1
    }
  }

  /**
    * Multiply a constant value to all positions of this buffer.
    */
  def multiply(v: Double): Unit = {
    var pos = 0
    while (pos < values.length) {
      values(pos) = values(pos) * v
      pos += 1
    }
  }

  /**
    * Divide the corresponding positions of the two buffers. The buffers must
    * have the same settings. The tags for the new buffer will be the
    * intersection.
    */
  def divide(ts: TimeSeriesBuffer): Unit = {
    val nts = ts.normalize(step, start, values.length)
    val length = scala.math.min(values.length, nts.values.length)
    var pos = 0
    while (pos < length) {
      values(pos) = values(pos) / nts.values(pos)
      pos += 1
    }
  }

  /**
    * Divide a constant value to all positions of this buffer.
    */
  def divide(v: Double): Unit = {
    var pos = 0
    while (pos < values.length) {
      values(pos) = values(pos) / v
      pos += 1
    }
  }

  /**
    * Updates each position we the max value between the two buffers. The
    * buffers must have the same settings. The tags for the new buffer will be
    * the intersection.
    */
  def max(ts: TimeSeriesBuffer): Unit = {
    val nts = ts.normalize(step, start, values.length)
    val length = scala.math.min(values.length, nts.values.length)
    var pos = 0
    while (pos < length) {
      val v1 = values(pos)
      val v2 = nts.values(pos)
      values(pos) = Math.maxNaN(v1, v2)
      pos += 1
    }
  }

  def max(block: Block): Int = {
    aggrBlock(block, Block.Max, ConsolidationFunction.Sum, 1, Math.maxNaN)
  }

  /**
    * Updates each position we the min value between the two buffers. The
    * buffers must have the same settings. The tags for the new buffer will be
    * the intersection.
    */
  def min(ts: TimeSeriesBuffer): Unit = {
    val nts = ts.normalize(step, start, values.length)
    val length = scala.math.min(values.length, nts.values.length)
    var pos = 0
    while (pos < length) {
      val v1 = values(pos)
      val v2 = nts.values(pos)
      values(pos) = Math.minNaN(v1, v2)
      pos += 1
    }
  }

  def min(block: Block): Int = {
    aggrBlock(block, Block.Min, ConsolidationFunction.Sum, 1, Math.minNaN)
  }

  /**
    * Setup this buffer array as a count.
    */
  def initCount(): Unit = {
    var pos = 0
    while (pos < values.length) {
      values(pos) = if (JDouble.isNaN(values(pos))) 0.0 else 1.0
      pos += 1
    }
  }

  /**
    * Updates each position by 1 if the corresponding position in the other
    * buffer is a valid number, i.e., is not equal NaN. The buffers must have
    * the same settings. The tags for the new buffer will be the intersection.
    */
  def count(ts: TimeSeriesBuffer): Unit = {
    val nts = ts.normalize(step, start, values.length)
    val length = scala.math.min(values.length, nts.values.length)
    var pos = 0
    while (pos < length) {
      val v1 = values(pos)
      val v2 = if (JDouble.isNaN(nts.values(pos))) 0.0 else 1.0
      values(pos) = v1 + v2
      pos += 1
    }
  }

  def count(block: Block): Int = {
    aggrBlock(block, Block.Count, ConsolidationFunction.Sum, 1, Math.addNaN)
  }

  /**
    * Merge with another metric buffer.
    */
  def merge(ts: TimeSeriesBuffer): Unit = {
    require(step == ts.step, "step sizes must be the same")
    require(start == ts.start, "start times must be the same")
    val length = math.min(values.length, ts.values.length)
    var i = 0
    while (i < length) {
      val v1 = values(i)
      val v2 = ts.values(i)
      if (JDouble.isNaN(v1) || v1 < v2) values(i) = v2
      i += 1
    }
  }

  /**
    * Returns a new buffer with values consolidated to a larger step size.
    */
  def consolidate(multiple: Int, cf: ConsolidationFunction): TimeSeriesBuffer = {
    val e = start + step * values.length
    val newStep = step * multiple
    val seq = new MapStepTimeSeq(data, newStep, cf).bounded(start, e)
    new TimeSeriesBuffer(tags, seq)
  }

  /**
    * Returns a new buffer with values un-consolidated to a smaller step size.
    */
  def unConsolidate(s: Long, e: Long, step: Long): TimeSeriesBuffer = {
    val diff = e - s
    val size = (diff / step + (if (diff % step != 0) 1 else 0)).asInstanceOf[Int]
    val buffer = new Array[Double](size)
    var i = 0
    while (i < buffer.length) {
      buffer(i) = getValue(s + i * step)
      i += 1
    }
    new TimeSeriesBuffer(tags, new ArrayTimeSeq(DsType.Gauge, s, step, buffer))
  }

  /**
    * Returns a new buffer normalized to the specified settings.
    */
  def normalize(step: Long, start: Long, size: Int): TimeSeriesBuffer = {
    val buf =
      if (step > this.step) consolidate((step / this.step).toInt, ConsolidationFunction.Avg)
      else this
    if (buf.start == start && buf.step == step) buf
    else {
      val buffer = new Array[Double](size)
      var i = 0
      while (i < buffer.length) {
        buffer(i) = buf.getValue(start + i * step)
        i += 1
      }
      new TimeSeriesBuffer(tags, new ArrayTimeSeq(dsType, start / step * step, step, buffer))
    }
  }

  def getValue(timestamp: Long): Double = {
    val offset = timestamp - start
    val pos = (offset / step).toInt
    if (offset < 0 || pos >= values.length) Double.NaN else values(pos)
  }

  override def equals(other: Any): Boolean = {

    // Follows guidelines from: http://www.artima.com/pins1ed/object-equality.html#28.4
    other match {
      case that: TimeSeriesBuffer =>
        that.canEqual(this) && tags == that.tags && data == that.data
      case _ => false
    }
  }

  override def hashCode: Int = {
    val prime = 31
    var hc = prime
    hc = hc * prime + tags.hashCode()
    hc = hc * prime + data.hashCode()
    hc
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[TimeSeriesBuffer]
  }

  override def toString: String = s"TimeSeriesBuffer($tags, $data)"

}
