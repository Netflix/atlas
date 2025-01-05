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

import java.time.Duration

import munit.FunSuite

class TrendSuite extends FunSuite {

  private val dataTags = Map("name" -> "cpu", "node" -> "i-1")

  private def alignedStream(step: Long): List[List[Datapoint]] = List(
    List(Datapoint(dataTags, 0L * step, 1.0, step)),
    List(Datapoint(dataTags, 1L * step, 1.5, step)),
    List(Datapoint(dataTags, 2L * step, 1.6, step)),
    List(Datapoint(dataTags, 3L * step, 1.7, step)),
    List(Datapoint(dataTags, 4L * step, 1.4, step)),
    List(Datapoint(dataTags, 5L * step, 1.3, step)),
    List(Datapoint(dataTags, 6L * step, 1.2, step)),
    List(Datapoint(dataTags, 7L * step, 1.0, step)),
    List(Datapoint(dataTags, 8L * step, 0.0, step)),
    List(Datapoint(dataTags, 9L * step, 0.0, step)),
    List(Datapoint(dataTags, 10L * step, 1.0, step)),
    List(Datapoint(dataTags, 11L * step, 1.1, step)),
    List(Datapoint(dataTags, 12L * step, 1.2, step)),
    List(Datapoint(dataTags, 13L * step, 1.2, step))
  )

  private def alignedInputTS(step: Long): TimeSeries = TimeSeries(
    dataTags,
    new ArrayTimeSeq(
      DsType.Gauge,
      0L,
      step,
      Array[Double](1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    )
  )

  private def trend(step: Long): StatefulExpr.Trend = StatefulExpr.Trend(
    DataExpr.Sum(Query.Equal("name", "cpu")),
    Duration.ofMillis(step * 3)
  )

  def eval(
    step: Long,
    expr: TimeSeriesExpr,
    data: List[List[Datapoint]]
  ): List[List[TimeSeries]] = {
    var state = Map.empty[StatefulExpr, Any]
    data.map { ts =>
      val t = ts.head.timestamp
      val context = EvalContext(t, t + step, step, state)
      val rs = expr.eval(context, ts)
      state = rs.state
      rs.data
    }
  }

  private def incrementalMatchesGlobal(step: Long): Unit = {
    val s = 0L
    val e = 14L * step
    val context = EvalContext(s, e, step, Map.empty)
    val expected = trend(step)
      .eval(context, List(alignedInputTS(step)))
      .data
      .head
      .data
      .bounded(s, e)
      .data
    val result = eval(step, trend(step), alignedStream(step))

    result.zip(expected).zipWithIndex.foreach {
      case ((ts, v), i) =>
        assertEquals(ts.size, 1)
        ts.foreach { t =>
          val r = t.data(i * step)
          if (i <= 1)
            assert(r.isNaN)
          else
            assertEqualsDouble(v, r, 0.00001)
        }
    }
  }

  test("trend: incremental exec matches global, 1ms") {
    incrementalMatchesGlobal(1L)
  }

  test("trend: incremental exec matches global, 50ms") {
    incrementalMatchesGlobal(50L)
  }

  test("trend: incremental exec matches global, 1m") {
    incrementalMatchesGlobal(60000L)
  }
}
