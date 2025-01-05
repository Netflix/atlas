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

import java.time.Instant

import com.netflix.atlas.core.util.Math

// TimeSeries can be lazy or eager. By default manipulations are done as a view over another
// time series. This view can be materialized for a given range by calling the bounded method.
trait TimeSeq {

  // start/end
  // iterate over time

  def dsType: DsType

  /**  */
  def step: Long

  def apply(timestamp: Long): Double

  def mapValues(f: Double => Double): TimeSeq = new UnaryOpTimeSeq(this, f)

  /** Fast loop with no intermediate object creation. */
  def foreach(s: Long, e: Long)(f: (Long, Double) => Unit): Unit = {
    require(s <= e, "start must be <= end")
    val end = e / step * step
    var t = s / step * step
    while (t < end) {
      f(t, apply(t))
      t += step
    }
  }

  def bounded(s: Long, e: Long): ArrayTimeSeq = {
    require(s <= e, "start must be <= end")
    val end = e / step * step
    val start = s / step * step
    val length = ((end - start) / step).toInt
    val data = new Array[Double](length)
    var i = 0
    foreach(start, end) { (_, v) =>
      data(i) = v
      i += 1
    }
    new ArrayTimeSeq(dsType, start, step, data)
  }
}

final class ArrayTimeSeq(
  val dsType: DsType,
  val start: Long,
  val step: Long,
  val data: Array[Double]
) extends TimeSeq {

  require(start % step == 0, "start time must be on step boundary")

  def end: Long = start + data.length * step

  def apply(timestamp: Long): Double = {
    val i = (timestamp - start) / step
    if (timestamp < start || timestamp >= end) Double.NaN else data(i.toInt)
  }

  /**
    * This overload is to improve performance when updating with another implementation of the
    * same class. It will restrict the update to the shared range between the two time series
    * and uses array lookups directly in the core loop to avoid expensive operations on long
    * values when using the timestamp as the index.
    */
  def update(ts: ArrayTimeSeq)(op: BinaryOp): Unit = {
    require(step == ts.step, "step sizes must be the same")
    val s = math.max(start, ts.start)
    val e = math.min(end, ts.end)
    if (s < e) {
      var i1 = ((s - start) / step).toInt
      var i2 = ((s - ts.start) / step).toInt
      val epos = ((e - start) / step).toInt
      while (i1 < epos) {
        data(i1) = op(data(i1), ts.data(i2))
        i1 += 1
        i2 += 1
      }
    }
  }

  def update(ts: TimeSeq)(op: BinaryOp): Unit = {
    require(step == ts.step, "step sizes must be the same")
    var i = 0
    ts.foreach(start, end) { (_, v) =>
      data(i) = op(data(i), v)
      i += 1
    }
  }

  def update(op: Double => Double): Unit = {
    var i = 0
    while (i < data.length) {
      data(i) = op(data(i))
      i += 1
    }
  }

  override def equals(other: Any): Boolean = {

    // Follows guidelines from: http://www.artima.com/pins1ed/object-equality.html#28.4
    other match {
      case that: ArrayTimeSeq =>
        that.canEqual(this) &&
        dsType == that.dsType &&
        step == that.step &&
        start == that.start &&
        java.util.Arrays.equals(data, that.data)
      case _ => false
    }
  }

  override def hashCode: Int = {
    import java.lang.Long as JLong
    val prime = 31
    var hc = prime
    hc = hc * prime + dsType.hashCode()
    hc = hc * prime + JLong.valueOf(step).hashCode()
    hc = hc * prime + JLong.valueOf(start).hashCode()
    hc = hc * prime + java.util.Arrays.hashCode(data)
    hc
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[ArrayTimeSeq]
  }

  override def toString: String = {
    val s = Instant.ofEpochMilli(start)
    val values = data.mkString("[", ",", "]")
    s"ArrayTimeSeq($dsType,$s,$step,$values)"
  }
}

class FunctionTimeSeq(val dsType: DsType, val step: Long, f: Long => Double) extends TimeSeq {

  def apply(timestamp: Long): Double = f(timestamp / step * step)
}

class OffsetTimeSeq(seq: TimeSeq, offset: Long) extends TimeSeq {

  def dsType: DsType = seq.dsType

  def step: Long = seq.step

  def apply(timestamp: Long): Double = seq(timestamp - offset)
}

class MapStepTimeSeq(ts: TimeSeq, val step: Long, cf: ConsolidationFunction) extends TimeSeq {

  import com.netflix.atlas.core.model.ConsolidationFunction.*

  private val isConsolidation = step > ts.step

  require(
    if (isConsolidation) step % ts.step == 0 else ts.step % step == 0,
    "consolidated step must be multiple of primary step"
  )

  private val consolidate = cf match {
    case Sum => Math.addNaN _
    case Avg => Math.addNaN _
    case Max => Math.maxNaN _
    case Min => Math.minNaN _
  }

  def dsType: DsType = ts.dsType

  def apply(timestamp: Long): Double = {
    if (isConsolidation) {
      val t = timestamp / step * step
      val m = (step / ts.step).toInt
      var i = 0
      var v = Double.NaN
      var c = 0
      while (i < m) {
        val n = ts(t + i * ts.step)
        v = consolidate(v, n)
        if (!n.isNaN) c += 1
        i += 1
      }

      // If it is a rate the average rate per second for the consolidated data point should
      // consider the entire interval. For gauges the value is sampled and we don't want a false
      // drop so use the intervals with data. Example, gauge showing current temperature that
      // starts reporting in the middle of an consolidated interval has primary points NaN, 90.0.
      // The consolidated value should be 90.0 not 45.0.
      val denominator = if (dsType == DsType.Rate) m else c
      if (c > 0 && cf == Avg) v / denominator else v
    } else {
      val t = timestamp / ts.step * ts.step
      ts(t)
    }
  }
}

class UnaryOpTimeSeq(ts: TimeSeq, f: UnaryOp) extends TimeSeq {

  def dsType: DsType = ts.dsType

  def step: Long = ts.step

  def apply(timestamp: Long): Double = f(ts(timestamp))
}

class BinaryOpTimeSeq(ts1: TimeSeq, ts2: TimeSeq, op: BinaryOp) extends TimeSeq {

  require(ts1.step == ts2.step, "time series must have the same step size")

  def dsType: DsType = ts1.dsType

  def step: Long = ts1.step

  def apply(timestamp: Long): Double = op(ts1(timestamp), ts2(timestamp))
}
