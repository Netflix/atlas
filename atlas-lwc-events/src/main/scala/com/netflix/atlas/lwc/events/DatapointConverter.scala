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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.util.Strings
import com.netflix.iep.config.ConfigManager
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.histogram.PercentileBuckets
import com.netflix.spectator.impl.StepDouble
import com.typesafe.scalalogging.StrictLogging

import java.time.Duration
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

/**
  * Helper to convert a sequence of events into a data point.
  */
private[events] trait DatapointConverter {

  /** Update the data point with an event. */
  def update(event: LwcEvent): Unit

  /** Update with a specific value that is already extracted from an event. */
  def update(value: Double): Unit

  /** Flush the data for a given timestamp. */
  def flush(timestamp: Long): Unit

  /** Returns true if the converter has no recent data. */
  def hasNoData: Boolean
}

private[events] object DatapointConverter {

  /**
    * Limit on the number of groups for a group by expression. If exceeded new groups
    * will be dropped.
    */
  private val MaxGroupBySize: Int = ConfigManager.get().getInt("atlas.lwc.events.max-groups")

  def apply(
    id: String,
    rawExpr: String,
    expr: DataExpr,
    clock: Clock,
    step: Long,
    sampleMapper: Option[LwcEvent => List[Any]],
    consumer: (String, LwcEvent) => Unit
  ): DatapointConverter = {
    val tags = Query.tags(expr.query)
    val mapper = createValueMapper(tags)
    val params =
      Params(id, rawExpr, Query.tags(expr.query), clock, step, mapper, sampleMapper, consumer)
    toConverter(expr, params)
  }

  private def toConverter(expr: DataExpr, params: Params): DatapointConverter = {
    expr match {
      case _: DataExpr.Sum      => Sum(params)
      case _: DataExpr.Count    => Count(params)
      case _: DataExpr.Max      => Max(params)
      case _: DataExpr.Min      => Min(params)
      case by: DataExpr.GroupBy => GroupBy(by, params)
      case _                    => Sum(params)
    }
  }

  /**
    * Extract value and map as needed based on the type. Uses statistic and grouping to
    * coerce events so they structurally work like spectator composite types.
    */
  private def createValueMapper(tags: Map[String, String]): LwcEvent => Double = {
    tags.get("value") match {
      case Some(k) =>
        tags.get("statistic") match {
          case Some("count")          => _ => 1.0
          case Some("totalOfSquares") => event => squared(event.extractValueSafe(k), event.value)
          case _                      => event => toDouble(event.extractValueSafe(k), event.value)
        }
      case None =>
        event => toDouble(event.value, 1.0)
    }
  }

  private def squared(value: Any, dflt: Any): Double = {
    val v = toDouble(value, dflt)
    v * v
  }

  @scala.annotation.tailrec
  private[events] def toDouble(value: Any, dflt: Any): Double = {
    value match {
      case v: Boolean  => if (v) 1.0 else 0.0
      case v: Byte     => v.toDouble
      case v: Short    => v.toDouble
      case v: Int      => v.toDouble
      case v: Long     => v.toDouble
      case v: Float    => v.toDouble
      case v: Double   => v
      case v: Number   => v.doubleValue()
      case v: String   => parseDouble(v)
      case v: Duration => v.toNanos / 1e9
      case _           => toDouble(dflt, 1.0)
    }
  }

  private def parseDouble(v: String): Double = {
    try {
      java.lang.Double.parseDouble(v)
    } catch {
      case _: Exception => Double.NaN
    }
  }

  private[events] def addNaN(now: Long, value: StepDouble, amount: Double): Unit = {
    if (amount.isNaN)
      return

    var set = false
    while (!set) {
      val v = value.getCurrent(now)
      if (v.isNaN) {
        set = value.compareAndSet(now, v, amount)
      } else {
        value.addAndGet(now, amount)
        set = true
      }
    }
  }

  case class Params(
    id: String,
    rawExpr: String,
    tags: Map[String, String],
    clock: Clock,
    step: Long,
    valueMapper: LwcEvent => Double,
    sampleMapper: Option[LwcEvent => List[Any]],
    consumer: (String, LwcEvent) => Unit
  )

  /** Compute sum for a counter as a rate per second. */
  case class Sum(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(Double.NaN, params.clock, params.step)

    private val sampleMapper: LwcEvent => List[Any] = params.sampleMapper.orNull
    @volatile private var sample: List[Any] = Nil
    @volatile private var sampleStale: Boolean = true

    override def update(event: LwcEvent): Unit = {
      if (sampleMapper != null && sampleStale) {
        sampleStale = false
        sample = sampleMapper(event)
      }
      update(params.valueMapper(event))
    }

    override def update(value: Double): Unit = {
      if (value.isFinite && value >= 0.0) {
        addNaN(params.clock.wallTime(), buffer, value)
      }
    }

    private def getAndResetSample(): List[List[Any]] = {
      if (sampleMapper == null) {
        Nil
      } else {
        // Mark the sample as stale so it will get refreshed roughly once per
        // step interval
        sampleStale = true
        List(sample)
      }
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.pollAsRate(timestamp)
      if (value.isFinite) {
        val s = getAndResetSample()
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value, s)
        params.consumer(params.id, event)
      }
    }

    override def hasNoData: Boolean = {
      val now = params.clock.wallTime()
      buffer.getCurrent(now).isNaN && buffer.poll(now).isNaN
    }
  }

  /** Compute count of contributing events. */
  case class Count(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(Double.NaN, params.clock, params.step)

    override def update(event: LwcEvent): Unit = {
      update(1.0)
    }

    override def update(value: Double): Unit = {
      addNaN(params.clock.wallTime(), buffer, 1.0)
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }

    override def hasNoData: Boolean = {
      val now = params.clock.wallTime()
      buffer.getCurrent(now).isNaN && buffer.poll(now).isNaN
    }
  }

  /** Compute max value from contributing events. */
  case class Max(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(Double.NaN, params.clock, params.step)

    override def update(event: LwcEvent): Unit = {
      update(params.valueMapper(event))
    }

    override def update(value: Double): Unit = {
      buffer.max(params.clock.wallTime(), value)
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }

    override def hasNoData: Boolean = {
      val now = params.clock.wallTime()
      buffer.getCurrent(now).isNaN && buffer.poll(now).isNaN
    }
  }

  /** Compute min value from contributing events. */
  case class Min(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(Double.NaN, params.clock, params.step)

    override def update(event: LwcEvent): Unit = {
      update(params.valueMapper(event))
    }

    override def update(value: Double): Unit = {
      buffer.min(params.clock.wallTime(), value)
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }

    override def hasNoData: Boolean = {
      val now = params.clock.wallTime()
      buffer.getCurrent(now).isNaN && buffer.poll(now).isNaN
    }
  }

  /** Compute set of data points, one for each distinct group. */
  case class GroupBy(by: DataExpr.GroupBy, params: Params)
      extends DatapointConverter
      with StrictLogging {

    private val groups = new ConcurrentHashMap[Map[String, String], DatapointConverter]()

    private val isPercentile = by.keys.contains(TagKey.percentile)

    @volatile private var maxGroupsExceeded: Boolean = false

    override def update(event: LwcEvent): Unit = {
      if (isPercentile) {
        val rawValue = getRawValue(event)
        val pctTag = toPercentileTag(rawValue)
        val tagValues = by.keys
          .map {
            case TagKey.percentile => pctTag
            case k                 => event.tagValue(k)
          }
          .filterNot(_ == null)
        getConverter(tagValues).foreach(_.update(1.0))
      } else {
        val tagValues = by.keys.map(event.tagValue).filterNot(_ == null)
        getConverter(tagValues).foreach(_.update(event))
      }
    }

    override def update(value: Double): Unit = {
      // Since there are no values for the group by tags, this is a no-op
    }

    private def getConverter(tagValues: List[String]): Option[DatapointConverter] = {
      // Ignore events that are missing dimensions used in the grouping
      if (by.keys.size == tagValues.size) {
        val tags = by.keys.zip(tagValues).toMap
        // If the max has been exceeded, allow existing groups to get updates, but do not
        // create new ones.
        if (maxGroupsExceeded)
          Option(groups.get(tags))
        else
          Some(groups.computeIfAbsent(tags, groupConverter))
      } else {
        None
      }
    }

    private def getRawValue(event: LwcEvent): Any = {
      params.tags.get("value") match {
        case Some(k) => event.extractValueSafe(k)
        case None    => event.value
      }
    }

    private def toPercentileTag(value: Any): String = {
      value match {
        case d: Duration => toPercentileHex("T", d.toNanos)
        case v           => toPercentileHex("D", toDouble(v, 1.0).longValue)
      }
    }

    private def toPercentileHex(prefix: String, value: Long): String = {
      if (value <= 0) {
        null
      } else {
        val b = PercentileBuckets.indexOf(value)
        val hex = Integer.toString(b, 16).toUpperCase(Locale.US)
        s"$prefix${Strings.zeroPad(hex, 4)}"
      }
    }

    private def groupConverter(tags: Map[String, String]): DatapointConverter = {
      val ps = params.copy(consumer = (id, event) => groupConsumer(tags, id, event))
      toConverter(by.af, ps)
    }

    private def groupConsumer(tags: Map[String, String], id: String, event: LwcEvent): Unit = {
      event match {
        case dp: DatapointEvent => params.consumer(id, dp.copy(tags = dp.tags ++ tags))
        case ev                 => params.consumer(id, ev)
      }
    }

    override def flush(timestamp: Long): Unit = {
      val it = groups.values().iterator()
      while (it.hasNext) {
        val converter = it.next()
        converter.flush(timestamp)
        if (converter.hasNoData)
          it.remove()
      }
      if (groups.size() >= MaxGroupBySize) {
        maxGroupsExceeded = true
        logger.info(s"max groups exceeded for expression: ${params.rawExpr}")
      }
    }

    override def hasNoData: Boolean = groups.isEmpty
  }
}
