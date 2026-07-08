/*
 * Copyright 2014-2026 Netflix, Inc.
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

class CumulativeMaxSuite extends FunSuite {

  private val interpreter = Interpreter(StatefulVocabulary.allWords)

  private val start = 0L
  private val step = 60000L

  private def ts(tags: Map[String, String], values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, values.toArray)
    TimeSeries(tags, seq)
  }

  private def ts(values: Double*): TimeSeries = ts(Map("name" -> "test"), values*)

  private def eval(input: TimeSeries, n: Int): TimeSeries = {
    val context = EvalContext(start, start + step * n, step)
    val expr = StatefulExpr.CumulativeMax(DataExpr.Sum(Query.True))
    expr.eval(context, List(input)).data.head
  }

  test("increasing") {
    val input = ts(1.0, 2.0, 3.0)
    assertEquals(eval(input, 3).data, ts(1.0, 2.0, 3.0).data)
  }

  test("decreasing keeps the running max") {
    val input = ts(3.0, 2.0, 1.0)
    assertEquals(eval(input, 3).data, ts(3.0, 3.0, 3.0).data)
  }

  test("mixed") {
    val input = ts(1.0, 3.0, 2.0, 5.0, 0.0)
    assertEquals(eval(input, 5).data, ts(1.0, 3.0, 3.0, 5.0, 5.0).data)
  }

  test("NaN values ignored") {
    val input = ts(Double.NaN, 2.0, Double.NaN, 1.0, 5.0)
    assertEquals(eval(input, 5).data, ts(Double.NaN, 2.0, 2.0, 2.0, 5.0).data)
  }

  test("negative values") {
    val input = ts(-5.0, -9.0, -2.0)
    assertEquals(eval(input, 3).data, ts(-5.0, -5.0, -2.0).data)
  }

  test("all NaN series stays NaN") {
    val input = ts(Double.NaN, Double.NaN, Double.NaN)
    assertEquals(eval(input, 3).data, ts(Double.NaN, Double.NaN, Double.NaN).data)
  }

  test("per series state when grouped") {
    // Two series: running max is tracked independently per series.
    val a = ts(Map("name" -> "test", "id" -> "a"), 1.0, 5.0, 2.0)
    val b = ts(Map("name" -> "test", "id" -> "b"), 9.0, 3.0, 4.0)
    val context = EvalContext(start, start + step * 3, step)
    val expr = StatefulExpr.CumulativeMax(DataExpr.GroupBy(DataExpr.Sum(Query.True), List("id")))
    val rs = expr.eval(context, List(a, b)).data
    val byId = rs.map(t => t.tags("id") -> t.data).toMap
    assertEquals(byId("a"), ts(1.0, 5.0, 5.0).data)
    assertEquals(byId("b"), ts(9.0, 9.0, 9.0).data)
  }

  test("round trip toString") {
    val expr = interpreter.execute("name,test,:eq,:sum,:cumulative-max").stack match {
      case (v: TimeSeriesExpr) :: Nil => v
      case _                          => fail("unexpected stack")
    }
    assertEquals(expr.toString, "name,test,:eq,:sum,:cumulative-max")
  }
}
