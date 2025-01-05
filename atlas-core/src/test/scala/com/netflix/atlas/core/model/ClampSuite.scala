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

import munit.FunSuite

class ClampSuite extends FunSuite {

  private val step = 60000L
  private val dataTags = Map("name" -> "cpu", "node" -> "i-1")

  private val inputTS = TimeSeries(
    dataTags,
    new ArrayTimeSeq(
      DsType.Gauge,
      0L,
      step,
      Array[Double](1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0)
    )
  )

  def eval(expr: TimeSeriesExpr, data: List[List[Datapoint]]): List[List[TimeSeries]] = {
    var state = Map.empty[StatefulExpr, Any]
    data.map { ts =>
      val t = ts.head.timestamp
      val context = EvalContext(t, t + step, step, state)
      val rs = expr.eval(context, ts)
      state = rs.state
      rs.data
    }
  }

  test("clamp-min") {
    val s = 0L
    val e = 10L * step
    val context = EvalContext(s, e, step, Map.empty)
    val clamp = MathExpr.ClampMin(DataExpr.Sum(Query.Equal("name", "cpu")), 1.1)
    val actual = clamp.eval(context, List(inputTS)).data.head.data.bounded(s, e).data
    val expected = Array[Double](1.1, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.1, 1.1, 1.1)
    assertEquals(actual.toSeq, expected.toSeq)
  }

  test("clamp-max") {
    val s = 0L
    val e = 10L * step
    val context = EvalContext(s, e, step, Map.empty)
    val clamp = MathExpr.ClampMax(DataExpr.Sum(Query.Equal("name", "cpu")), 1.1)
    val actual = clamp.eval(context, List(inputTS)).data.head.data.bounded(s, e).data
    val expected = Array[Double](1.0, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.0, 0.0, 0.0)
    assertEquals(actual.toSeq, expected.toSeq)
  }

}
