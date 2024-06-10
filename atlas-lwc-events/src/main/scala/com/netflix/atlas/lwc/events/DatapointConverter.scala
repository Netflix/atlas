/*
 * Copyright 2014-2024 Netflix, Inc.
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
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.histogram.PercentileBuckets
import com.netflix.spectator.impl.AtomicDouble
import com.netflix.spectator.impl.StepDouble

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
}

private[events] object DatapointConverter {

  def apply(
    id: String,
    expr: DataExpr,
    clock: Clock,
    step: Long,
    consumer: (String, LwcEvent) => Unit
  ): DatapointConverter = {
    val tags = Query.tags(expr.query)
    val mapper = createValueMapper(tags)
    val params = Params(id, Query.tags(expr.query), clock, step, mapper, consumer)
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
          case Some("totalOfSquares") => event => squared(event.extractValue(k), event.value)
          case _                      => event => toDouble(event.extractValue(k), event.value)
        }
      case None =>
        event => event.value
    }
  }

  private def squared(value: Any, dflt: Double): Double = {
    val v = toDouble(value, dflt)
    v * v
  }

  private[events] def toDouble(value: Any, dflt: Double): Double = {
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
      case _           => dflt
    }
  }

  private def parseDouble(v: String): Double = {
    try {
      java.lang.Double.parseDouble(v)
    } catch {
      case _: Exception => Double.NaN
    }
  }

  case class Params(
    id: String,
    tags: Map[String, String],
    clock: Clock,
    step: Long,
    valueMapper: LwcEvent => Double,
    consumer: (String, LwcEvent) => Unit
  )

  /** Compute sum for a counter as a rate per second. */
  case class Sum(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(0.0, params.clock, params.step)

    override def update(event: LwcEvent): Unit = {
      update(params.valueMapper(event))
    }

    override def update(value: Double): Unit = {
      if (value.isFinite && value >= 0.0) {
        buffer.getCurrent.addAndGet(value)
      }
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.pollAsRate(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }
  }

  /** Compute count of contributing events. */
  case class Count(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(0.0, params.clock, params.step)

    override def update(event: LwcEvent): Unit = {
      buffer.getCurrent.addAndGet(1.0)
    }

    override def update(value: Double): Unit = {
      buffer.getCurrent.addAndGet(1.0)
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }
  }

  /** Compute max value from contributing events. */
  case class Max(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(Double.NaN, params.clock, params.step)

    override def update(event: LwcEvent): Unit = {
      update(params.valueMapper(event))
    }

    override def update(value: Double): Unit = {
      if (value.isFinite) {
        buffer.getCurrent.max(value)
      }
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }
  }

  /** Compute min value from contributing events. */
  case class Min(params: Params) extends DatapointConverter {

    private val buffer = new StepDouble(Double.NaN, params.clock, params.step)

    override def update(event: LwcEvent): Unit = {
      update(params.valueMapper(event))
    }

    override def update(value: Double): Unit = {
      if (value.isFinite) {
        min(buffer.getCurrent, value)
      }
    }

    private def min(current: AtomicDouble, value: Double): Unit = {
      if (value.isFinite) {
        var min = current.get()
        while (isLessThan(value, min) && !current.compareAndSet(min, value)) {
          min = current.get()
        }
      }
    }

    private def isLessThan(v1: Double, v2: Double): Boolean = {
      v1 < v2 || v2.isNaN
    }

    override def flush(timestamp: Long): Unit = {
      val value = buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }
  }

  /** Compute set of data points, one for each distinct group. */
  case class GroupBy(by: DataExpr.GroupBy, params: Params) extends DatapointConverter {

    private val groups = new ConcurrentHashMap[Map[String, String], DatapointConverter]()

    private val isPercentile = by.keys.contains(TagKey.percentile)

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
        update(1.0, tagValues, event)
      } else {
        val tagValues = by.keys.map(event.tagValue).filterNot(_ == null)
        val value = params.valueMapper(event)
        update(value, tagValues, event)
      }
    }

    override def update(value: Double): Unit = {
      // Since there are no values for the group by tags, this is a no-op
    }

    private def update(value: Double, tagValues: List[String], event: LwcEvent): Unit = {
      // Ignore events that are missing dimensions used in the grouping
      if (by.keys.size == tagValues.size) {
        if (value.isFinite) {
          val tags = by.keys.zip(tagValues).toMap
          val converter = groups.computeIfAbsent(tags, groupConverter)
          converter.update(value)
        }
      }
    }

    private def getRawValue(event: LwcEvent): Any = {
      params.tags.get("value") match {
        case Some(k) => event.extractValue(k)
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
      groups.values().forEach(_.flush(timestamp))
    }
  }
}
