/*
 * Copyright 2014-2019 Netflix, Inc.
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

import com.netflix.atlas.core.util.IdentityMap
import org.scalatest.funsuite.AnyFunSuite

class DerivativeSuite extends AnyFunSuite {

  private val start = 0L
  private val step = 60000L

  def ts(values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, values.toArray)
    TimeSeries(Map("name" -> "test"), seq)
  }

  def eval(input: TimeSeries, n: Int): TimeSeries = {
    val context = EvalContext(start, start + step * n, step)
    val expr = StatefulExpr.Derivative(DataExpr.Sum(Query.True))
    expr.eval(context, List(input)).data.head
  }

  test("basic") {
    val input = ts(7.0, 8.0, 9.0)
    assert(eval(input, 3).data === ts(Double.NaN, 1.0, 1.0).data)
  }

  test("basic with same value and decreasing") {
    val input = ts(7.0, 42.0, 42.0, 43.0, 2.0, 5.0)
    assert(eval(input, 6).data === ts(Double.NaN, 35.0, 0.0, 1.0, -41.0, 3.0).data)
  }

  test("basic with NaN values") {
    val input = ts(7.0, 42.0, Double.NaN, 43.0, 2.0, 5.0)
    assert(eval(input, 6).data === ts(Double.NaN, 35.0, Double.NaN, Double.NaN, -41.0, 3.0).data)
  }

  test("state across binary operations") {
    val d1 = StatefulExpr.Derivative(DataExpr.Sum(Query.True))
    val d2 = StatefulExpr.Derivative(DataExpr.Sum(Query.True))
    val expr = MathExpr.Add(d1, d2)

    var state: Map[StatefulExpr, Any] = IdentityMap.empty[StatefulExpr, Any]
    List(1.0, 2.0, 3.0, 4.0, 5.0).zipWithIndex.foreach {
      case (v, i) =>
        val s = start + step * i
        val e = s + step
        val context = EvalContext(s, e, step, state)
        val seq = new ArrayTimeSeq(DsType.Gauge, s, step, Array(v))
        val input = TimeSeries(Map("name" -> "test"), seq)
        val result = expr.eval(context, List(input))
        state = result.state

        val actual = result.data.head.data.bounded(s, e)
        val expectedValue = if (i == 0) Double.NaN else 2.0
        val expected = new ArrayTimeSeq(DsType.Gauge, s, step, Array(expectedValue))
        assert(actual === expected)
    }
  }

  test("state will expire and get cleaned up") {
    val expr = StatefulExpr.Derivative(DataExpr.Sum(Query.True))

    val context = EvalContext(start, start + step * 4, step)
    val rs = expr.eval(context, List(ts(1.0, 2.0, 3.0, Double.NaN)))

    assert(rs.state(expr).asInstanceOf[scala.collection.mutable.Map[_, _]].isEmpty)
  }
}
