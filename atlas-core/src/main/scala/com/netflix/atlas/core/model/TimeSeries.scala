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
package com.netflix.atlas.core.model

import java.math.BigInteger

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
    if (tags.isEmpty) "NO TAGS" else {
      toLabel(tags.keys.toList.sortWith(_ < _), tags)
    }
  }

  def toLabel(keys: List[String], tags: Map[String, String]): String = {
    val str = keys.map(k => s"$k=${tags.getOrElse(k, "NULL")}").mkString(", ")
    if (keys.size == 1) str else s"$str"
  }

  def aggregate(ds: Iterator[TimeSeries], start: Long, end: Long, f: BinaryOp): TimeSeries = {
    require(ds.nonEmpty, "must have 1 or more time series to perform aggregation")
    val init = ds.next()
    val buf = init.data.bounded(start, end)
    var tags = init.tags
    while (ds.hasNext) {
      val t = ds.next()
      tags = TaggedItem.aggrTags(tags, t.tags)
      buf.update(t.data)(f)
    }
    TimeSeries(tags, buf)
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
    LazyTimeSeries(tags, s, data)
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

case class BasicTimeSeries(
    id: BigInteger,
    tags: Map[String, String],
    label: String,
    data: TimeSeq) extends TimeSeries

case class LazyTimeSeries(
    tags: Map[String, String],
    label: String,
    data: TimeSeq) extends TimeSeries {
  lazy val id: BigInteger = TaggedItem.computeId(tags)
}

