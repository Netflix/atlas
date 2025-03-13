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

import com.netflix.atlas.core.util.Math

object TimeSeries {

  private val noDataTags = Map("name" -> "NO_DATA")
  private val noDataId = TaggedItem.computeId(noDataTags)

  /**
    * Create a time series with all `NaN` values representing that there were no matches for
    * the query. The tags will only have a name with a value of "NO_DATA".
    *
    * @param step
    *     Step size to use for the returned time series.
    * @return
    *     Time series with `NaN` for all values.
    */
  def noData(step: Long): TimeSeries = {
    val data = new FunctionTimeSeq(DsType.Gauge, step, _ => Double.NaN)
    BasicTimeSeries(noDataId, noDataTags, "NO DATA", data)
  }

  /**
    * Create a time series with all `NaN` values representing that there were no matches for
    * the query. The tags will be extracted from the query.
    *
    * @param query
    *     Query that did not match any time series.
    * @param step
    *     Step size to use for the returned time series.
    * @return
    *     Time series with `NaN` for all values.
    */
  def noData(query: Query, step: Long): TimeSeries = {
    val tags = Query.tags(query)
    val data = new FunctionTimeSeq(DsType.Gauge, step, _ => Double.NaN)
    LazyTimeSeries(if (tags.isEmpty) noDataTags else tags, "NO DATA", data)
  }

  def apply(tags: Map[String, String], label: String, data: TimeSeq): TimeSeries = {
    LazyTimeSeries(tags, label, data)
  }

  def apply(tags: Map[String, String], data: TimeSeq): TimeSeries = {
    TimeSeries(tags, toLabel(tags), data)
  }

  def toLabel(tags: Map[String, String]): String = {
    if (tags.isEmpty) "NO TAGS"
    else {
      toLabel(tags.keys.toList.sortWith(_ < _), tags)
    }
  }

  def toLabel(keys: List[String], tags: Map[String, String]): String = {
    val str = keys.map(k => s"$k=${tags.getOrElse(k, "NULL")}").mkString(", ")
    if (keys.size == 1) str else s"$str"
  }

  /**
    * Base type for aggregators that can be used to combine a set of time series using
    * some aggregation function. The aggregation is performed in-place on the buffer so
    * it requires a bounded time range.
    */
  trait Aggregator {

    /** Start of bounded range. */
    def start: Long

    /** End of bounded range. */
    def end: Long

    /** Update the aggregation with another time series. */
    def update(t: TimeSeries): Unit

    /** Check if the aggregation is empty, that is no time series have been added. */
    def isEmpty: Boolean

    /** Returns the aggregated time series. */
    def result(): TimeSeries
  }

  /** No-operation aggregator that can be used when aggregation is optional. */
  case object NoopAggregator extends Aggregator {

    def start: Long = 0L
    def end: Long = 0L

    override def update(t: TimeSeries): Unit = ()

    override def isEmpty: Boolean = true

    override def result(): TimeSeries = {
      throw new UnsupportedOperationException
    }
  }

  /**
    * Simple aggregator that can combine the corresponding values using a basic math
    * function.
    */
  class SimpleAggregator(val start: Long, val end: Long, f: BinaryOp) extends Aggregator {

    private[this] var aggrBuffer: ArrayTimeSeq = _
    private[this] var aggrTags: Map[String, String] = _

    override def update(t: TimeSeries): Unit = {
      if (aggrBuffer == null) {
        aggrBuffer = t.data.bounded(start, end)
        aggrTags = t.tags
      } else {
        aggrBuffer.update(t.data)(f)
      }
    }

    override def isEmpty: Boolean = {
      aggrBuffer == null
    }

    override def result(): TimeSeries = {
      require(aggrBuffer != null, "must have 1 or more time series to perform aggregation")
      TimeSeries(aggrTags, aggrBuffer)
    }
  }

  /**
    * Aggregation that computes the number of time series that have a value for
    * a given interval.
    */
  class CountAggregator(val start: Long, val end: Long) extends Aggregator {

    private[this] var aggrBuffer: ArrayTimeSeq = _
    private[this] var aggrTags: Map[String, String] = _

    override def update(t: TimeSeries): Unit = {
      if (aggrBuffer == null) {
        aggrBuffer = t.data
          .mapValues(v => if (v.isNaN) Double.NaN else 1.0)
          .bounded(start, end)
        aggrTags = t.tags
      } else {
        aggrBuffer.update(t.data)(countNaN)
      }
    }

    private def countNaN(v1: Double, v2: Double): Double = {
      if (v2.isNaN) v1 else Math.addNaN(v1, 1.0)
    }

    override def isEmpty: Boolean = {
      aggrBuffer == null
    }

    override def result(): TimeSeries = {
      require(aggrBuffer != null, "must have 1 or more time series to perform aggregation")
      TimeSeries(aggrTags, aggrBuffer)
    }
  }

  /**
    * Aggregator that computes the average for the input time series.
    */
  class AvgAggregator(val start: Long, val end: Long) extends Aggregator {

    private[this] val sumAggregator = new SimpleAggregator(start, end, Math.addNaN)
    private[this] val countAggregator = new CountAggregator(start, end)

    override def update(t: TimeSeries): Unit = {
      sumAggregator.update(t)
      countAggregator.update(t)
    }

    override def isEmpty: Boolean = {
      sumAggregator.isEmpty
    }

    override def result(): TimeSeries = {
      val sum = sumAggregator.result()
      val count = countAggregator.result()
      val seq = new BinaryOpTimeSeq(sum.data, count.data, _ / _)
      TimeSeries(sum.tags, seq)
    }
  }
}

// TimeSeries can be lazy or eager. By default manipulations are done as a view over another
// time series. This view can be materialized for a given range by calling the bounded method.
trait TimeSeries extends TaggedItem {

  def label: String

  def data: TimeSeq

  def datapoint(timestamp: Long): Datapoint = {
    Datapoint(tags, timestamp, data(timestamp))
  }

  def unaryOp(labelFmt: String, f: Double => Double): TimeSeries = {
    LazyTimeSeries(tags, labelFmt.format(label), new UnaryOpTimeSeq(data, f))
  }

  def binaryOp(ts: TimeSeries, labelFmt: String, f: BinaryOp): TimeSeries = {
    val newData = new BinaryOpTimeSeq(data, ts.data, f)
    LazyTimeSeries(tags, labelFmt.format(label, ts.label), newData)
  }

  def withTags(ts: Map[String, String]): TimeSeries = {
    LazyTimeSeries(ts, label, data)
  }

  def withLabel(s: String): TimeSeries = {
    // If the specified label is empty, then fallback to the default
    if (s.isEmpty) this else LazyTimeSeries(tags, s, data)
  }

  def consolidate(step: Long, cf: ConsolidationFunction): TimeSeries = {
    val newData = new MapStepTimeSeq(data, step, cf)
    LazyTimeSeries(tags, label, newData)
  }

  def blend(ts: TimeSeries): TimeSeries = {
    val newData = new BinaryOpTimeSeq(data, ts.data, Math.maxNaN)
    LazyTimeSeries(ts.tags, ts.label, newData)
  }

  // Create a copy with a modified time sequence.
  def mapTimeSeq(f: TimeSeq => TimeSeq): TimeSeries = {
    LazyTimeSeries(tags, label, f(data))
  }

  def offset(dur: Long): TimeSeries = {
    LazyTimeSeries(tags, label, new OffsetTimeSeq(data, dur))
  }
}

case class BasicTimeSeries(id: ItemId, tags: Map[String, String], label: String, data: TimeSeq)
    extends TimeSeries

case class LazyTimeSeries(tags: Map[String, String], label: String, data: TimeSeq)
    extends TimeSeries {
  lazy val id: ItemId = TaggedItem.computeId(tags)
}
