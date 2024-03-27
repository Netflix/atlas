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
import com.netflix.atlas.core.model.EventExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TraceQuery
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.atlas.impl.QueryIndex

import java.util.concurrent.atomic.AtomicLong

abstract class AbstractLwcEventClient(clock: Clock) extends LwcEventClient {

  import AbstractLwcEventClient.*

  @volatile private var handlers: List[EventHandler] = _

  @volatile private var index: QueryIndex[EventHandler] = QueryIndex.newInstance(new NoopRegistry)

  @volatile private var traceHandlers: Map[Subscription, TraceQuery.SpanFilter] = Map.empty

  protected def sync(subscriptions: Subscriptions): Unit = {
    val flushableHandlers = List.newBuilder[EventHandler]
    val idx = QueryIndex.newInstance[EventHandler](new NoopRegistry)

    // Pass-through events
    subscriptions.passThrough.foreach { sub =>
      val expr = ExprUtils.parseEventExpr(sub.expression)
      val q = removeValueClause(expr.query)
      val handler = expr match {
        case EventExpr.Raw(_)       => EventHandler(sub, e => List(e))
        case EventExpr.Table(_, cs) => EventHandler(sub, e => List(LwcEvent.Row(e, cs)))
      }
      idx.add(q, handler)
    }

    // Analytics based on events
    subscriptions.analytics.foreach { sub =>
      val expr = ExprUtils.parseDataExpr(sub.expression)
      val converter = DatapointConverter(sub.id, expr, clock, sub.step, submit)
      val q = removeValueClause(expr.query)
      val handler = EventHandler(
        sub,
        event => {
          converter.update(event)
          Nil
        },
        Some(converter)
      )
      idx.add(q, handler)
      flushableHandlers += handler
    }

    // Trace pass-through
    traceHandlers = subscriptions.tracePassThrough.map { sub =>
      sub -> ExprUtils.parseTraceEventsQuery(sub.expression)
    }.toMap

    index = idx
    handlers = flushableHandlers.result()
  }

  private def removeValueClause(query: Query): SpectatorQuery = {
    val q = query
      .rewrite {
        case kq: Query.KeyQuery if kq.k == "value" => Query.True
      }
      .asInstanceOf[Query]
    ExprUtils.toSpectatorQuery(Query.simplify(q, ignore = true))
  }

  override def process(event: LwcEvent): Unit = {
    event match {
      case LwcEvent.HeartbeatLwcEvent(timestamp) =>
        handlers.foreach(_.flush(timestamp))
      case _ =>
        index.forEachMatch(k => event.tagValue(k), h => handleMatch(event, h))
    }
  }

  private def handleMatch(event: LwcEvent, handler: EventHandler): Unit = {
    handler.mapper(event).foreach { e =>
      submit(handler.subscription.id, e)
    }
  }

  override def processTrace(trace: Seq[LwcEvent.Span]): Unit = {
    traceHandlers.foreachEntry { (sub, filter) =>
      if (TraceMatcher.matches(filter.q, trace)) {
        val filtered = trace.filter(event => ExprUtils.matches(filter.f, event.tagValue))
        if (filtered.nonEmpty) {
          submit(sub.id, LwcEvent.Events(filtered))
        }
      }
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

    def flush(timestamp: Long): Unit = {
      val stepTime = timestamp / subscription.step
      if (stepTime > lastFlushTimestamp.get()) {
        converter.foreach(_.flush(timestamp))
        lastFlushTimestamp.set(stepTime)
      }
    }
  }
}
