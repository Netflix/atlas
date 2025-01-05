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

class DesSuite extends FunSuite {

  private val step = 60000L
  private val dataTags = Map("name" -> "cpu", "node" -> "i-1")

  private val alignedStream = List(
    List(Datapoint(dataTags, 0L * step, 1.0)),
    List(Datapoint(dataTags, 1L * step, 1.5)),
    List(Datapoint(dataTags, 2L * step, 1.6)),
    List(Datapoint(dataTags, 3L * step, 1.7)),
    List(Datapoint(dataTags, 4L * step, 1.4)),
    List(Datapoint(dataTags, 5L * step, 1.3)),
    List(Datapoint(dataTags, 6L * step, 1.2)),
    List(Datapoint(dataTags, 7L * step, 1.0)),
    List(Datapoint(dataTags, 8L * step, 0.0)),
    List(Datapoint(dataTags, 9L * step, 0.0)),
    List(Datapoint(dataTags, 10L * step, 1.0)),
    List(Datapoint(dataTags, 11L * step, 1.1)),
    List(Datapoint(dataTags, 12L * step, 1.2)),
    List(Datapoint(dataTags, 13L * step, 1.2))
  )

  private val alignedInputTS = TimeSeries(
    dataTags,
    new ArrayTimeSeq(
      DsType.Gauge,
      0L,
      step,
      Array[Double](1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    )
  )

  private val unalignedStream = List(
    List(Datapoint(dataTags, 1L * step, 1.5)),
    List(Datapoint(dataTags, 2L * step, 1.6)),
    List(Datapoint(dataTags, 3L * step, 1.7)),
    List(Datapoint(dataTags, 4L * step, 1.4)),
    List(Datapoint(dataTags, 5L * step, 1.3)),
    List(Datapoint(dataTags, 6L * step, 1.2)),
    List(Datapoint(dataTags, 7L * step, 1.0)),
    List(Datapoint(dataTags, 8L * step, 0.0)),
    List(Datapoint(dataTags, 9L * step, 0.0)),
    List(Datapoint(dataTags, 10L * step, 1.0)),
    List(Datapoint(dataTags, 11L * step, 1.1)),
    List(Datapoint(dataTags, 12L * step, 1.2)),
    List(Datapoint(dataTags, 13L * step, 1.2))
  )

  private val unalignedInputTS = TimeSeries(
    dataTags,
    new ArrayTimeSeq(
      DsType.Gauge,
      1L * step,
      step,
      Array[Double](1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    )
  )

  private val des = StatefulExpr.Des(DataExpr.Sum(Query.Equal("name", "cpu")), 2, 0.1, 0.02)
  private val sdes = StatefulExpr.SlidingDes(DataExpr.Sum(Query.Equal("name", "cpu")), 2, 0.1, 0.02)

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

  test("des: incremental exec matches global") {
    val s = 0L
    val e = 14L * step
    val context = EvalContext(s, e, step, Map.empty)
    val expected = des.eval(context, List(alignedInputTS)).data.head.data.bounded(s, e).data

    val result = eval(des, alignedStream)
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

  test("sdes: aligned incremental exec matches global") {
    val s = 0L
    val e = 14L * step
    val context = EvalContext(s, e, step, Map.empty)
    val expected = sdes.eval(context, List(alignedInputTS)).data.head.data.bounded(s, e).data

    val result = eval(sdes, alignedStream)
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

  test("sdes: unaligned incremental exec matches global") {
    val s = 1L * step // offset by one step, half a training window used
    val e = 14L * step
    val context = EvalContext(s, e, step, Map.empty)
    val expected = sdes.eval(context, List(unalignedInputTS)).data.head.data.bounded(s, e).data

    val result = eval(sdes, unalignedStream)
    // println(expected.mkString(", "))
    // println(result.map { case v => v(0).data.asInstanceOf[ArrayTimeSeq].data(0) }.mkString(", "))
    result.zip(expected).zipWithIndex.foreach {
      case ((ts, v), i) =>
        assertEquals(ts.size, 1)
        ts.foreach { t =>
          val r = t.data((i + 1) * step) // offset step by our skipped data
          if (i <= 2)
            assert(r.isNaN)
          else
            assertEqualsDouble(v, r, 0.00001)
        }
    }
  }
}
