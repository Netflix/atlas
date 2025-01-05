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

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TraceQuery

/**
  * Re-usable matcher to determine if a trace query matches a sequence of spans.
  */
trait TraceMatcher {

  /** Update the matcher by checking the given span. */
  def check(event: LwcEvent.Span): Unit

  /**
    * Returns true if the trace query matches. Should only be called after all the spans
    * have been checked.
    */
  def matches: Boolean

  /** Reset the matcher so it can be re-used. */
  def reset(): Unit
}

object TraceMatcher {

  /**
    * Check if a trace query matches the trace.
    *
    * @param query
    *     Expression for whether a trace should be selected.
    * @param trace
    *     Set of spans that are part of a call tree.
    * @return
    *     True if the query matches the trace.
    */
  def matches(query: TraceQuery, trace: Seq[LwcEvent.Span]): Boolean = {
    val matcher = apply(query)
    trace.foreach(matcher.check)
    matcher.matches
  }

  /** Construct a new matcher for the trace query. */
  def apply(query: TraceQuery): TraceMatcher = {
    query match {
      case TraceQuery.Child(q1, q2)   => new ChildMatcher(q1, q2)
      case TraceQuery.SpanAnd(q1, q2) => new SpanAndMatcher(apply(q1), apply(q2))
      case TraceQuery.SpanOr(q1, q2)  => new SpanOrMatcher(apply(q1), apply(q2))
      case TraceQuery.Simple(q)       => new SimpleMatcher(q)
    }
  }

  private class SimpleMatcher(query: Query) extends TraceMatcher {

    private var matched: Boolean = false

    override def check(event: LwcEvent.Span): Unit = {
      matched = matched || ExprUtils.matches(query, event.tagValue)
    }

    override def matches: Boolean = matched

    override def reset(): Unit = {
      matched = false
    }
  }

  private class SpanAndMatcher(m1: TraceMatcher, m2: TraceMatcher) extends TraceMatcher {

    override def check(event: LwcEvent.Span): Unit = {
      m1.check(event)
      m2.check(event)
    }

    override def matches: Boolean = m1.matches && m2.matches

    override def reset(): Unit = {
      m1.reset()
      m2.reset()
    }
  }

  private class SpanOrMatcher(m1: TraceMatcher, m2: TraceMatcher) extends TraceMatcher {

    override def check(event: LwcEvent.Span): Unit = {
      m1.check(event)
      m2.check(event)
    }

    override def matches: Boolean = m1.matches || m2.matches

    override def reset(): Unit = {
      m1.reset()
      m2.reset()
    }
  }

  private class ChildMatcher(parent: Query, child: Query) extends TraceMatcher {

    private val parentSpanIds = collection.mutable.HashSet.empty[String]
    private val childParentIds = collection.mutable.HashSet.empty[String]

    private var matched = false

    override def check(event: LwcEvent.Span): Unit = {
      if (matched)
        return

      if (ExprUtils.matches(parent, event.tagValue)) {
        val id = event.spanId
        if (childParentIds.contains(id)) {
          matched = true
          return
        } else {
          parentSpanIds.add(id)
        }
      }

      if (ExprUtils.matches(child, event.tagValue)) {
        val id = event.parentId
        if (parentSpanIds.contains(id)) {
          matched = true
          return
        } else {
          childParentIds.add(id)
        }
      }
    }

    override def matches: Boolean = matched

    override def reset(): Unit = {
      parentSpanIds.clear()
      childParentIds.clear()
      matched = false
    }
  }
}
