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

class StreamSuite extends FunSuite {

  private val stream = List(
    List(
      Datapoint(Map("name" -> "cpu", "node" -> "i-1"), 0L, 42.0)
    ),
    List(
      Datapoint(Map("name" -> "cpu", "node" -> "i-1"), 10L, 42.0),
      Datapoint(Map("name" -> "cpu", "node" -> "i-2"), 10L, 42.0),
      Datapoint(Map("name" -> "cpu", "node" -> "i-3"), 10L, 42.0)
    ),
    List(
      Datapoint(Map("name" -> "cpu", "node" -> "i-1"), 20L, 42.0),
      Datapoint(Map("name" -> "cpu", "node" -> "i-2"), 20L, 42.0)
    ),
    List(
      Datapoint(Map("name" -> "cpu", "node" -> "i-2"), 30L, 42.0)
    )
  )

  def eval(expr: TimeSeriesExpr, data: List[List[Datapoint]]): List[List[TimeSeries]] = {
    val state = Map.empty[StatefulExpr, Any]
    data.map { ts =>
      val t = ts.head.timestamp
      val context = EvalContext(t, t + 10, 10, state)
      expr.eval(context, ts).data
    }
  }

  test("sum: same tags for all intervals") {
    val aggr = DataExpr.Sum(Query.Equal("name", "cpu"))
    val result = eval(aggr, stream)
    result.foreach { ts =>
      assertEquals(ts.size, 1)
      ts.foreach { t =>
        assertEquals(t.tags, Map("name" -> "cpu"))
      }
    }
  }

  test("count: same tags for all intervals") {
    val aggr = DataExpr.Count(Query.Equal("name", "cpu"))
    val result = eval(aggr, stream)
    result.foreach { ts =>
      assertEquals(ts.size, 1)
      ts.foreach { t =>
        assertEquals(t.tags, Map("name" -> "cpu"))
      }
    }
  }

  test("by: name,cpu,:eq,(,name,),:by") {
    val aggr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "cpu")), List("name"))
    val result = eval(aggr, stream)
    result.foreach { ts =>
      assertEquals(ts.size, 1)
      ts.foreach { t =>
        assertEquals(t.tags, Map("name" -> "cpu"))
      }
    }
  }

  test("by: name,cpu,:eq,(,node,),:by") {
    val aggr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "cpu")), List("node"))
    val result = eval(aggr, stream)
    result.zipWithIndex.foreach {
      case (ts, i) =>
        val expected = i match {
          case 0 => Set(Map("name" -> "cpu", "node" -> "i-1"))
          case 1 => (1 to 3).map(i => Map("name" -> "cpu", "node" -> s"i-$i")).toSet
          case 2 => (1 to 2).map(i => Map("name" -> "cpu", "node" -> s"i-$i")).toSet
          case 3 => Set(Map("name" -> "cpu", "node" -> "i-2"))
        }
        assertEquals(expected, ts.map(_.tags).toSet)
    }
  }

  test("by: :true,(,name,),:by") {
    val aggr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("name"))
    val result = eval(aggr, stream)
    result.foreach { ts =>
      assertEquals(ts.size, 1)
      ts.foreach { t =>
        assertEquals(t.tags, Map("name" -> "cpu"))
      }
    }
  }

  test("by: :true,(,name,node,),:by") {
    val aggr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("name", "node"))
    val result = eval(aggr, stream)
    result.zipWithIndex.foreach {
      case (ts, i) =>
        val expected = i match {
          case 0 => Set(Map("name" -> "cpu", "node" -> "i-1"))
          case 1 => (1 to 3).map(i => Map("name" -> "cpu", "node" -> s"i-$i")).toSet
          case 2 => (1 to 2).map(i => Map("name" -> "cpu", "node" -> s"i-$i")).toSet
          case 3 => Set(Map("name" -> "cpu", "node" -> "i-2"))
        }
        assertEquals(expected, ts.map(_.tags).toSet)
    }
  }

  test("unknown for if all values can change") {
    // Values that are not explicitly fixed by the query could change if new data comes in
    // with different values for those tags. To keep the data consistent, we must restrict
    // to only values that are fixed based on the query.
    val sum = DataExpr.Sum(Query.True)
    val result = eval(sum, stream)
    result.foreach { ts =>
      assertEquals(ts.size, 1)
      ts.foreach { t =>
        assertEquals(t.tags, Map("name" -> "unknown"))
      }
    }
  }
}
