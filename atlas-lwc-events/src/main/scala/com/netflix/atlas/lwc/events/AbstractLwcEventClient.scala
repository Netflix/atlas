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

import com.netflix.atlas.core.model.EventExpr
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.atlas.impl.QueryIndex

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

abstract class AbstractLwcEventClient(clock: Clock, filter: LwcEventFilter) extends LwcEventClient {

  import AbstractLwcEventClient.*

  private val subHandlers = new ConcurrentHashMap[Subscription, (SpectatorQuery, EventHandler)]()

  private val index: QueryIndex[EventHandler] = QueryIndex.newInstance(new NoopRegistry)

  @volatile private var currentSubs: Subscriptions = Subscriptions()

  @volatile private var handlers: List[EventHandler] = Nil

  /**
    * Called to force flushing the data. Implementations should override if they have
    * some buffering.
    */
  protected def flush(): Unit = {}

  protected def sync(subscriptions: Subscriptions): Unit = {
    val diff = Subscriptions.diff(currentSubs, subscriptions)
    currentSubs = subscriptions

    val flushableHandlers = List.newBuilder[EventHandler]

    // Pass-through events
    diff.added.events.foreach { sub =>
      val expr = ExprUtils.parseEventExpr(sub.expression)
      val queries = filter.splitQuery(expr.query)
      val q = ExprUtils.toSpectatorQuery(queries.indexQuery)
      val handler = expr match {
        case EventExpr.Raw(_)       => EventHandler(sub, e => List(e))
        case EventExpr.Table(_, cs) => EventHandler(sub, e => List(LwcEvent.Row(e, cs)))
        case expr: EventExpr.Sample =>
          val converter = DatapointConverter(
            filter.valueDimension,
            sub.id,
            sub.expression,
            expr.dataExpr,
            clock,
            sub.step,
            Some(event => expr.projectionKeys.map(event.extractValueSafe)),
            submit
          )
          EventHandler(
            sub,
            event => {
              if (filter.matches(event, queries.postFilterQuery))
                converter.update(event)
              Nil
            },
            Some(converter)
          )
      }
      index.add(q, handler)
      subHandlers.put(sub, q -> handler)
      if (handler.converter.isDefined)
        flushableHandlers += handler
    }
    diff.unchanged.events.foreach { sub =>
      val handlerMeta = subHandlers.get(sub)
      if (handlerMeta != null && handlerMeta._2.converter.isDefined)
        flushableHandlers += handlerMeta._2
    }
    diff.removed.events.foreach(removeSubscription)

    // Analytics based on events
    diff.added.timeSeries.foreach { sub =>
      val expr = ExprUtils.parseDataExpr(sub.expression)
      val converter = DatapointConverter(
        filter.valueDimension,
        sub.id,
        sub.expression,
        expr,
        clock,
        sub.step,
        None,
        submit
      )
      val queries = filter.splitQuery(expr.query)
      val q = ExprUtils.toSpectatorQuery(queries.indexQuery)
      val handler = EventHandler(
        sub,
        event => {
          if (filter.matches(event, queries.postFilterQuery))
            converter.update(event)
          Nil
        },
        Some(converter)
      )
      index.add(q, handler)
      subHandlers.put(sub, q -> handler)
      flushableHandlers += handler
    }
    diff.unchanged.timeSeries.foreach { sub =>
      val handlerMeta = subHandlers.get(sub)
      if (handlerMeta != null)
        flushableHandlers += handlerMeta._2
    }
    diff.removed.timeSeries.foreach(removeSubscription)

    handlers = flushableHandlers.result()
  }

  private def removeSubscription(sub: Subscription): Unit = {
    val handlerMeta = subHandlers.remove(sub)
    if (handlerMeta != null) {
      val (q, handler) = handlerMeta
      index.remove(q, handler)
    }
  }

  override def couldMatch(tags: String => String): Boolean = {
    index.couldMatch(k => tags(k))
  }

  override def process(event: LwcEvent): Unit = {
    event match {
      case LwcEvent.HeartbeatLwcEvent(timestamp) =>
        handlers.foreach(_.flush(timestamp))
        flush()
      case _ =>
        index.forEachMatch(k => event.tagValue(k), h => handleMatch(event, h))
    }
  }

  private def handleMatch(event: LwcEvent, handler: EventHandler): Unit = {
    handler.mapper(event).foreach { e =>
      submit(handler.subscription.id, e)
    }
  }
}

object AbstractLwcEventClient {

  private case class EventHandler(
    subscription: Subscription,
    mapper: LwcEvent => List[LwcEvent],
    converter: Option[DatapointConverter] = None
  ) {

    private val lastFlushTimestamp = new AtomicLong(0L)

    override val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)

    def flush(timestamp: Long): Unit = {
      val stepTime = timestamp / subscription.step
      if (stepTime > lastFlushTimestamp.get()) {
        converter.foreach(_.flush(timestamp))
        lastFlushTimestamp.set(stepTime)
      }
    }
  }
}
