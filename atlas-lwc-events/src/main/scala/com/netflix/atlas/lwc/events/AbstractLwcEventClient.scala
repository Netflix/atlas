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
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.atlas.impl.QueryIndex

abstract class AbstractLwcEventClient extends LwcEventClient {

  import AbstractLwcEventClient.*

  @volatile private var index: QueryIndex[EventHandler] = QueryIndex.newInstance(new NoopRegistry)

  @volatile private var traceHandlers: Map[Subscription, TraceQuery.SpanFilter] = Map.empty

  protected def sync(subscriptions: Subscriptions): Unit = {
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
      val q = removeValueClause(expr.query)
      val queryTags = Query.tags(expr.query)
      val handler = EventHandler(
        sub,
        event => {
          val tags = groupTags(expr, event)
          tags.fold(List.empty[LwcEvent]) { ts =>
            val tags = ts ++ queryTags
            val v = dataValue(tags, event)
            List(DatapointEvent(sub.id, SortedTagMap(tags), event.timestamp, v))
          }
        }
      )
      idx.add(q, handler)
    }

    // Trace pass-through
    traceHandlers = subscriptions.tracePassThrough.map { sub =>
      sub -> ExprUtils.parseTraceQuery(sub.expression)
    }.toMap

    index = idx
  }

  private def removeValueClause(query: Query): SpectatorQuery = {
    val q = query
      .rewrite {
        case kq: Query.KeyQuery if kq.k == "value" => Query.True
      }
      .asInstanceOf[Query]
    ExprUtils.toSpectatorQuery(Query.simplify(q, ignore = true))
  }

  private def groupTags(expr: DataExpr, event: LwcEvent): Option[Map[String, String]] = {
    val tags = Map.newBuilder[String, String]
    val it = expr.finalGrouping.iterator
    while (it.hasNext) {
      val k = it.next()
      val v = event.tagValue(k)
      if (v == null)
        return None
      else
        tags += k -> v
    }
    Some(tags.result())
  }

  private def dataValue(tags: Map[String, String], event: LwcEvent): Double = {
    tags.get("value").fold(1.0) { v =>
      event.extractValue(v) match {
        case b: Boolean if b => 1.0
        case _: Boolean      => 0.0
        case n: Byte         => n.toDouble
        case n: Short        => n.toDouble
        case n: Int          => n.toDouble
        case n: Long         => n.toDouble
        case n: Float        => n.toDouble
        case n: Double       => n
        case n: Number       => n.doubleValue()
        case _               => 1.0
      }
    }
  }

  override def process(event: LwcEvent): Unit = {
    index.forEachMatch(k => event.tagValue(k), h => handleMatch(event, h))
  }

  private def handleMatch(event: LwcEvent, handler: EventHandler): Unit = {
    handler.mapper(event).foreach { e =>
      submit(handler.subscription.id, e)
    }
  }

  override def processTrace(trace: Seq[LwcEvent.Span]): Unit = {
    traceHandlers.foreachEntry { (sub, filter) =>
      if (TraceMatcher.matches(filter.q, trace)) {
        val filtered = trace.filter(event => ExprUtils.matches(filter.f.query, event.tagValue))
        if (filtered.nonEmpty) {
          submit(sub.id, LwcEvent.Events(filtered))
        }
      }
    }
  }
}

object AbstractLwcEventClient {

  private case class EventHandler(subscription: Subscription, mapper: LwcEvent => List[LwcEvent])
}
