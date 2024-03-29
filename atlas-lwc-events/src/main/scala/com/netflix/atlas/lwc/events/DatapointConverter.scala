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
import com.netflix.spectator.api.Clock
import com.netflix.spectator.impl.AtomicDouble
import com.netflix.spectator.impl.StepDouble

import java.util.concurrent.ConcurrentHashMap

/**
  * Helper to convert a sequence of events into a data point.
  */
private[events] trait DatapointConverter {

  def update(event: LwcEvent): Unit

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
    val mapper = createValueMapper(tags, expr.finalGrouping)
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
  def createValueMapper(tags: Map[String, String], grouping: List[String]): LwcEvent => Double = {
    tags.get("value") match {
      case Some(k) =>
        tags.get("statistic") match {
          case Some("count")          => _ => 1.0
          case Some("totalOfSquares") => event => squared(event.extractValue(k))
          case _                      => event => toDouble(event.extractValue(k))
        }
      case None =>
        _ => 1.0
    }
  }

  private def squared(value: Any): Double = {
    val v = toDouble(value)
    v * v
  }

  def toDouble(value: Any): Double = {
    value match {
      case v: Boolean => if (v) 1.0 else 0.0
      case v: Byte    => v.toDouble
      case v: Short   => v.toDouble
      case v: Int     => v.toDouble
      case v: Long    => v.toDouble
      case v: Float   => v.toDouble
      case v: Double  => v
      case v: Number  => v.doubleValue()
      case v: String  => parseDouble(v)
      case _          => 1.0
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
  ) {

    val buffer = new StepDouble(0.0, clock, step)
  }

  /** Compute sum for a counter as a rate per second. */
  case class Sum(params: Params) extends DatapointConverter {

    override def update(event: LwcEvent): Unit = {
      val value = params.valueMapper(event)
      if (value.isFinite && value >= 0.0) {
        params.buffer.getCurrent.addAndGet(value)
      }
    }

    override def flush(timestamp: Long): Unit = {
      val value = params.buffer.pollAsRate(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }
  }

  /** Compute count of contributing events. */
  case class Count(params: Params) extends DatapointConverter {

    override def update(event: LwcEvent): Unit = {
      params.buffer.getCurrent.addAndGet(1.0)
    }

    override def flush(timestamp: Long): Unit = {
      val value = params.buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }
  }

  /** Compute max value from contributing events. */
  case class Max(params: Params) extends DatapointConverter {

    override def update(event: LwcEvent): Unit = {
      val value = params.valueMapper(event)
      if (value.isFinite && value >= 0.0) {
        params.buffer.getCurrent.max(value)
      }
    }

    override def flush(timestamp: Long): Unit = {
      val value = params.buffer.poll(timestamp)
      if (value.isFinite) {
        val ts = timestamp / params.step * params.step
        val event = DatapointEvent(params.id, params.tags, ts, value)
        params.consumer(params.id, event)
      }
    }
  }

  /** Compute min value from contributing events. */
  case class Min(params: Params) extends DatapointConverter {

    override def update(event: LwcEvent): Unit = {
      val value = params.valueMapper(event)
      if (value.isFinite && value >= 0.0) {
        min(params.buffer.getCurrent, value)
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
      val value = params.buffer.poll(timestamp)
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

    override def update(event: LwcEvent): Unit = {
      // Ignore events that are missing dimensions used in the grouping
      val values = by.keys.map(event.tagValue).filterNot(_ == null)
      if (by.keys.size == values.size) {
        val value = params.valueMapper(event)
        if (value.isFinite) {
          val tags = by.keys.zip(values).toMap
          val converter = groups.computeIfAbsent(tags, groupConverter)
          converter.update(event)
        }
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
