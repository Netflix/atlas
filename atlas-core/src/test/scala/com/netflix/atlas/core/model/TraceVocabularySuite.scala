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
import munit.FunSuite

class TraceVocabularySuite extends FunSuite {

  private val interpreter = new Interpreter(TraceVocabulary.allWords)

  private def parseTraceQuery(str: String): TraceQuery = {
    import ModelExtractors.*
    val expr = interpreter.execute(str).stack match {
      case TraceQueryType(t) :: Nil => t
      case _                        => throw new IllegalArgumentException(str)
    }
    assertEquals(expr.toString, str)
    expr
  }

  private def parseFilter(str: String): TraceQuery.SpanFilter = {
    val expr = interpreter.execute(str).stack match {
      case (t: TraceQuery.SpanFilter) :: Nil => t
      case _                                 => throw new IllegalArgumentException(str)
    }
    assertEquals(expr.toString, str)
    expr
  }

  private def parseTimeSeries(str: String): TraceQuery.SpanTimeSeries = {
    val expr = interpreter.execute(str).stack match {
      case (t: TraceQuery.SpanTimeSeries) :: Nil => t
      case _                                     => throw new IllegalArgumentException(str)
    }
    assertEquals(expr.toString, str)
    expr
  }

  test("simple Query coerced to TraceQuery") {
    val q = parseTraceQuery("app,foo,:eq")
    assertEquals(q, TraceQuery.Simple(Query.Equal("app", "foo")))
  }

  test("span-and") {
    val q = parseTraceQuery("app,foo,:eq,app,bar,:eq,:span-and")
    val expected = TraceQuery.SpanAnd(
      TraceQuery.Simple(Query.Equal("app", "foo")),
      TraceQuery.Simple(Query.Equal("app", "bar"))
    )
    assertEquals(q, expected)
  }

  test("span-or") {
    val q = parseTraceQuery("app,foo,:eq,app,bar,:eq,:span-or")
    val expected = TraceQuery.SpanOr(
      TraceQuery.Simple(Query.Equal("app", "foo")),
      TraceQuery.Simple(Query.Equal("app", "bar"))
    )
    assertEquals(q, expected)
  }

  test("child") {
    val q = parseTraceQuery("app,foo,:eq,app,bar,:eq,:child")
    val expected = TraceQuery.Child(
      Query.Equal("app", "foo"),
      Query.Equal("app", "bar")
    )
    assertEquals(q, expected)
  }

  test("span-filter") {
    val q = parseFilter("app,foo,:eq,app,bar,:eq,:child,app,foo,:eq,:span-filter")
    val expected = TraceQuery.SpanFilter(
      TraceQuery.Child(
        Query.Equal("app", "foo"),
        Query.Equal("app", "bar")
      ),
      Query.Equal("app", "foo")
    )
    assertEquals(q, expected)
  }

  test("span-time-series") {
    val q = parseTimeSeries("app,foo,:eq,app,bar,:eq,:child,app,foo,:eq,:sum,:span-time-series")
    val expected = TraceQuery.SpanTimeSeries(
      TraceQuery.Child(
        Query.Equal("app", "foo"),
        Query.Equal("app", "bar")
      ),
      StyleExpr(DataExpr.Sum(Query.Equal("app", "foo")), Map.empty)
    )
    assertEquals(q, expected)
  }
}
