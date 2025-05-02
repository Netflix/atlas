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

import com.netflix.atlas.core.stacklang.Interpreter

/** Base type for a query to match a trace. */
sealed trait TraceQuery extends Expr

object TraceQuery {

  /**
    * Wraps a Query type to be a TraceQuery. This will typically happen via an implicit
    * conversion when using as a parameter to a another operator that expects a TraceQuery.
    */
  case class Simple(query: Query) extends TraceQuery {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, query)
    }
  }

  /** Matches if the trace has a span that matches `q1` and a span that matches `q2`. */
  case class SpanAnd(q1: TraceQuery, q2: TraceQuery) extends TraceQuery {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q1, q2, Interpreter.WordToken(":span-and"))
    }
  }

  /** Matches if the trace has a span that matches `q1` or a span that matches `q2`. */
  case class SpanOr(q1: TraceQuery, q2: TraceQuery) extends TraceQuery {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q1, q2, Interpreter.WordToken(":span-or"))
    }
  }

  /**
    * Matches if the trace has a span that matches `q1` with a direct child span that
    * matches `q2`.
    */
  case class Child(q1: Query, q2: Query) extends TraceQuery {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q1, q2, Interpreter.WordToken(":child"))
    }
  }

  /** Filter to select the set of spans from a trace to forward as events. */
  case class SpanFilter(q: TraceQuery, f: Query) extends Expr {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q, f, Interpreter.WordToken(":span-filter"))
    }
  }

  /** Time series based on data from a set of matching traces. */
  case class SpanTimeSeries(q: TraceQuery, expr: StyleExpr) extends Expr {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q, expr, Interpreter.WordToken(":span-time-series"))
    }
  }
}
