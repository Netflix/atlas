/*
 * Copyright 2014-2018 Netflix, Inc.
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

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class TrendSuite extends FunSuite {

  val step = 60000L
  val dataTags = Map("name" -> "cpu", "node" -> "i-1")

  val alignedStream = List(
    List(Datapoint(dataTags,  0L * step, 1.0)),
    List(Datapoint(dataTags,  1L * step, 1.5)),
    List(Datapoint(dataTags,  2L * step, 1.6)),
    List(Datapoint(dataTags,  3L * step, 1.7)),
    List(Datapoint(dataTags,  4L * step, 1.4)),
    List(Datapoint(dataTags,  5L * step, 1.3)),
    List(Datapoint(dataTags,  6L * step, 1.2)),
    List(Datapoint(dataTags,  7L * step, 1.0)),
    List(Datapoint(dataTags,  8L * step, 0.0)),
    List(Datapoint(dataTags,  9L * step, 0.0)),
    List(Datapoint(dataTags, 10L * step, 1.0)),
    List(Datapoint(dataTags, 11L * step, 1.1)),
    List(Datapoint(dataTags, 12L * step, 1.2)),
    List(Datapoint(dataTags, 13L * step, 1.2))
  )
  val alignedInputTS = TimeSeries(dataTags, new ArrayTimeSeq(DsType.Gauge, 0L, step,
    Array[Double](1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)))

  val trend = StatefulExpr.Trend(DataExpr.Sum(Query.Equal("name" , "cpu")), Duration.ofNanos(step * 3 * 1000000))

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

  test("trend: incremental exec matches global") {
    val s = 0L
    val e = 14L * step
    val context = EvalContext(s, e, step, Map.empty)
    val expected = trend.eval(context, List(alignedInputTS)).data.head.data.bounded(s, e).data
    val result = eval(trend, alignedStream)

    result.zip(expected).zipWithIndex.foreach { case ((ts, v), i) =>
      assert(ts.size === 1)
      ts.foreach { t =>
        val r = t.data(i * step)
        if (i <= 1)
          assert(r.isNaN)
        else
          assert(v === r +- 0.00001)
      }
    }
  }
}
