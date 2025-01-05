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
import com.netflix.atlas.core.model.EventExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TraceQuery
import munit.FunSuite

class ExprUtilsSuite extends FunSuite {

  test("data: ok") {
    val expected = DataExpr.Sum(Query.Equal("app", "foo"))
    assertEquals(ExprUtils.parseDataExpr("app,foo,:eq,:sum"), expected)
  }

  test("data: invalid") {
    intercept[IllegalArgumentException] {
      ExprUtils.parseDataExpr("app,foo,:eq,:sum,1")
    }
  }

  test("event: raw") {
    val expected = EventExpr.Raw(Query.Equal("app", "foo"))
    assertEquals(ExprUtils.parseEventExpr("app,foo,:eq"), expected)
  }

  test("event: table") {
    val expected = EventExpr.Table(Query.Equal("app", "foo"), List("app"))
    assertEquals(ExprUtils.parseEventExpr("app,foo,:eq,(,app,),:table"), expected)
  }

  test("event: invalid") {
    intercept[IllegalArgumentException] {
      ExprUtils.parseEventExpr("app,foo,:eq,1")
    }
  }

  test("trace: simple query") {
    val expected = TraceQuery.SpanFilter(
      TraceQuery.Simple(Query.Equal("app", "foo")),
      Query.True
    )
    assertEquals(ExprUtils.parseTraceEventsQuery("app,foo,:eq"), expected)
  }

  test("trace: complex") {
    val expected = TraceQuery.SpanFilter(
      TraceQuery.Child(
        Query.Equal("app", "foo"),
        Query.Equal("app", "bar")
      ),
      Query.Equal("app", "foo")
    )
    val expr = "app,foo,:eq,app,bar,:eq,:child,app,foo,:eq,:span-filter"
    assertEquals(ExprUtils.parseTraceEventsQuery(expr), expected)
  }

  test("trace: analytics") {
    val expected = TraceQuery.SpanTimeSeries(
      TraceQuery.Child(
        Query.Equal("app", "foo"),
        Query.Equal("app", "bar")
      ),
      StyleExpr(DataExpr.Sum(Query.Equal("app", "foo")), Map.empty)
    )
    val expr = "app,foo,:eq,app,bar,:eq,:child,app,foo,:eq,:sum,:span-time-series"
    assertEquals(ExprUtils.parseTraceTimeSeriesQuery(expr), expected)
  }
}
