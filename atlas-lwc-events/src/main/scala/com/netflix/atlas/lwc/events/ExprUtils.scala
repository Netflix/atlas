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
import com.netflix.atlas.core.model.DataVocabulary
import com.netflix.atlas.core.model.EventExpr
import com.netflix.atlas.core.model.EventVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TraceQuery
import com.netflix.atlas.core.model.TraceVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.spectator.atlas.impl.Parser

/**
  * Helper functions for working with expressions.
  */
private[events] object ExprUtils {

  import ModelExtractors.*

  private val dataInterpreter = Interpreter(DataVocabulary.allWords)

  /** Parse a single data expression. */
  def parseDataExpr(str: String): DataExpr = {
    dataInterpreter.execute(str).stack match {
      case DataExprType(e) :: Nil => e
      case _                      => throw new IllegalArgumentException(str)
    }
  }

  private val eventInterpreter = Interpreter(EventVocabulary.allWords)

  /** Parse a single event expression. */
  def parseEventExpr(str: String): EventExpr = {
    eventInterpreter.execute(str).stack match {
      case EventExprType(e) :: Nil => e
      case _                       => throw new IllegalArgumentException(str)
    }
  }

  private val traceInterpreter = Interpreter(TraceVocabulary.allWords)

  /** Parse a single trace events query expression. */
  def parseTraceEventsQuery(str: String): TraceQuery.SpanFilter = {
    traceInterpreter.execute(str).stack match {
      case TraceFilterType(tq) :: Nil => tq
      case _                          => throw new IllegalArgumentException(str)
    }
  }

  /** Parse a single trace time series query expression. */
  def parseTraceTimeSeriesQuery(str: String): TraceQuery.SpanTimeSeries = {
    traceInterpreter.execute(str).stack match {
      case TraceTimeSeriesType(tq) :: Nil => tq
      case _                              => throw new IllegalArgumentException(str)
    }
  }

  /** Convert from Atlas query model to Spectator to use with a QueryIndex. */
  def toSpectatorQuery(query: Query): SpectatorQuery = {
    Parser.parseQuery(query.toString)
  }

  /**
    * Check if a query matches based on tags provided by a function.
    *
    * @param query
    *     Query to use for matching against the set of tags.
    * @param tags
    *     Function that returns the value for a given key. If there is no mapping
    *     for a given key, then it should return `null`.
    */
  def matches(query: Query, tags: String => String): Boolean = {
    query match {
      case Query.True             => true
      case Query.False            => false
      case q: Query.HasKey        => tags(q.k) != null
      case q: Query.KeyValueQuery => check(q, tags)
      case Query.And(q1, q2)      => matches(q1, tags) && matches(q2, tags)
      case Query.Or(q1, q2)       => matches(q1, tags) || matches(q2, tags)
      case Query.Not(q)           => !matches(q, tags)
    }
  }

  private def check(q: Query.KeyValueQuery, tags: String => String): Boolean = {
    val v = tags(q.k)
    v != null && q.check(v)
  }
}
