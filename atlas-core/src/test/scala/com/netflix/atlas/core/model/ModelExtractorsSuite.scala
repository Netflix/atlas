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

class ModelExtractorsSuite extends FunSuite {

  private val words = StyleVocabulary.allWords
  private val interpreter = Interpreter(words)

  private val candidates = words.filter(!_.matches(Nil))

  def completionTest(expr: String, expected: Int): Unit = {
    test(expr) {
      val result = interpreter.execute(expr)
      assertEquals(candidates.count(_.matches(result.stack)), expected)
    }
  }

  completionTest("name", 8)
  completionTest("name,sps", 22)
  completionTest("name,sps,:eq", 22)
  completionTest("name,sps,:eq,app,foo,:eq", 43)
  completionTest("name,sps,:eq,app,foo,:eq,:and,(,asg,)", 12)

  test("TraceQueryType: implicit from Query") {
    val value = Query.Equal("name", "sps")
    value match {
      case ModelExtractors.TraceQueryType(t) =>
        assertEquals(t, TraceQuery.Simple(value))
      case _ =>
        fail(s"did not match value: $value")
    }
  }

  test("TraceQueryType: explicit TraceQuery") {
    val value = TraceQuery.Simple(Query.Equal("name", "sps"))
    value match {
      case ModelExtractors.TraceQueryType(t) =>
        assertEquals(t, value)
      case _ =>
        fail(s"did not match value: $value")
    }
  }

  test("TraceFilterType: implicit from Query") {
    val value = Query.Equal("name", "sps")
    value match {
      case ModelExtractors.TraceFilterType(t) =>
        assertEquals(t, TraceQuery.SpanFilter(TraceQuery.Simple(value), Query.True))
      case _ =>
        fail(s"did not match value: $value")
    }
  }

  test("TraceFilterType: implicit from TraceQuery") {
    val value = TraceQuery.Simple(Query.Equal("name", "sps"))
    value match {
      case ModelExtractors.TraceFilterType(t) =>
        assertEquals(t, TraceQuery.SpanFilter(value, Query.True))
      case _ =>
        fail(s"did not match value: $value")
    }
  }

  test("TraceFilterType: explicit TraceQuery.SpanFilter") {
    val value = TraceQuery.SpanFilter(TraceQuery.Simple(Query.Equal("name", "sps")), Query.True)
    value match {
      case ModelExtractors.TraceFilterType(t) =>
        assertEquals(t, value)
      case _ =>
        fail(s"did not match value: $value")
    }
  }

  test("TraceTimeSeriesType: implicit from Query") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map.empty)
    val value = Query.Equal("name", "sps")
    value match {
      case ModelExtractors.TraceTimeSeriesType(t) =>
        assertEquals(t, TraceQuery.SpanTimeSeries(TraceQuery.Simple(value), expr))
      case _ =>
        fail(s"did not match value: $value")
    }
  }

  test("TraceTimeSeriesType: implicit from TraceQuery") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map.empty)
    val value = TraceQuery.Simple(Query.Equal("name", "sps"))
    value match {
      case ModelExtractors.TraceTimeSeriesType(t) =>
        assertEquals(t, TraceQuery.SpanTimeSeries(value, expr))
      case _ =>
        fail(s"did not match value: $value")
    }
  }

  test("TraceTimeSeriesType: explicit TraceQuery.SpanFilter") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map.empty)
    val value = TraceQuery.SpanTimeSeries(TraceQuery.Simple(Query.Equal("name", "sps")), expr)
    value match {
      case ModelExtractors.TraceTimeSeriesType(t) =>
        assertEquals(t, value)
      case _ =>
        fail(s"did not match value: $value")
    }
  }
}
