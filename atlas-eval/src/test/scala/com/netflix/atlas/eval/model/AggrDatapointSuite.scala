/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.eval.model

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.spectator.api.DefaultRegistry
import munit.FunSuite

class AggrDatapointSuite extends FunSuite {

  private val step = 60000
  private val registry = new DefaultRegistry()

  private def createDatapoints(expr: DataExpr, t: Long, nodes: Int): List[AggrDatapoint] = {
    (0 until nodes).toList.map { i =>
      val node = f"i-$i%08d"
      val tags = Map("name" -> "cpu")
      if (!expr.isInstanceOf[DataExpr.AggregateFunction])
        AggrDatapoint(t, step, expr, node, tags + ("node" -> node), i)
      else
        AggrDatapoint(t, step, expr, node, tags, i)
    }
  }

  test("aggregate empty") {
    assertEquals(
      AggrDatapoint.aggregate(Nil, Integer.MAX_VALUE, Integer.MAX_VALUE, registry),
      Option.empty
    )
  }

  test("aggregate simple") {
    val expr = DataExpr.Sum(Query.True)
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, Integer.MAX_VALUE, Integer.MAX_VALUE, registry)
    val result = aggregator.get.datapoints

    assertEquals(result.size, 1)
    assertEquals(result.head.timestamp, 0L)
    assertEquals(result.head.tags, Map("name" -> "cpu"))
    assertEquals(result.head.value, 45.0)
  }

  test("aggregate simple exceeds input data points limit") {
    val expr = DataExpr.Sum(Query.True)
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, 5, Integer.MAX_VALUE, registry)

    assert(aggregator.get.maxInputOrIntermediateDatapointsExceeded)
  }

  test("aggregate dedups using source") {
    val expr = DataExpr.Sum(Query.True)
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset ::: dataset, Integer.MAX_VALUE, Integer.MAX_VALUE, registry)
    val result = aggregator.get.datapoints

    assertEquals(result.size, 1)
    assertEquals(result.head.timestamp, 0L)
    assertEquals(result.head.tags, Map("name" -> "cpu"))
    assertEquals(result.head.value, 45.0)
  }

  test("aggregate group by") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, Integer.MAX_VALUE, Integer.MAX_VALUE, registry)
    val result = aggregator.get.datapoints

    assertEquals(result.size, 10)
    result.foreach { d =>
      val v = d.tags("node").substring(2).toDouble
      assertEquals(d.value, v)
    }
  }

  test("aggregate group by exceeds input data points") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, 5, Integer.MAX_VALUE, registry)

    assert(aggregator.get.maxInputOrIntermediateDatapointsExceeded)
  }

  test("aggregate group by exceeds intermediate data points") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, Integer.MAX_VALUE, 5, registry)

    assert(aggregator.get.maxInputOrIntermediateDatapointsExceeded)
  }

  test("aggregate, dedup and group by") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset ::: dataset, Integer.MAX_VALUE, Integer.MAX_VALUE, registry)
    val result = aggregator.get.datapoints

    assertEquals(result.size, 10)
    result.foreach { d =>
      val v = d.tags("node").substring(2).toDouble
      assertEquals(d.value, v)
    }
  }

  test("aggregate all") {
    val expr = DataExpr.All(Query.True)
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, Integer.MAX_VALUE, Integer.MAX_VALUE, registry)
    val result = aggregator.get.datapoints

    assertEquals(result.size, 10)
    result.foreach { d =>
      val v = d.tags("node").substring(2).toDouble
      assertEquals(d.value, v)
    }
  }

  test("aggregate all exceeds input data points") {
    val expr = DataExpr.All(Query.True)
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, 5, Integer.MAX_VALUE, registry)

    assert(aggregator.get.maxInputOrIntermediateDatapointsExceeded)
  }
}
