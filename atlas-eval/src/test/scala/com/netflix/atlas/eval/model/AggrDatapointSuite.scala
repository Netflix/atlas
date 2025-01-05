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
package com.netflix.atlas.eval.model

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.spectator.api.DefaultRegistry
import munit.FunSuite

class AggrDatapointSuite extends FunSuite {

  private val step = 60000
  private val registry = new DefaultRegistry()

  private def settings(maxInput: Int, maxIntermediate: Int): AggrDatapoint.AggregatorSettings = {
    AggrDatapoint.AggregatorSettings(maxInput, maxIntermediate, registry)
  }

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

  private def createGaugeDatapoints(expr: DataExpr, t: Long, nodes: Int): List[AggrDatapoint] = {
    (0 until nodes).toList.map { i =>
      val k = i % 2
      val node = f"i-$k%08d"
      val tags = Map("name" -> "cpu", "atlas.aggr" -> k.toString)
      if (!expr.isInstanceOf[DataExpr.AggregateFunction])
        AggrDatapoint(t, step, expr, i.toString, tags + ("node" -> node), i)
      else
        AggrDatapoint(t, step, expr, i.toString, tags, i)
    }
  }

  test("aggregate empty") {
    assertEquals(
      AggrDatapoint.aggregate(Nil, settings(Integer.MAX_VALUE, Integer.MAX_VALUE)),
      Option.empty
    )
  }

  test("aggregate simple") {
    val expr = DataExpr.Sum(Query.True)
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
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
      AggrDatapoint.aggregate(dataset, settings(5, Integer.MAX_VALUE))

    assert(aggregator.get.limitExceeded)
  }

  test("aggregate gauges sum") {
    val expr = DataExpr.Sum(Query.True)
    val dataset = createGaugeDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
    val result = aggregator.get.datapoints

    assertEquals(result.size, 1)
    assertEquals(result.head.timestamp, 0L)
    assertEquals(result.head.tags, Map("name" -> "cpu"))
    assertEquals(result.head.value, 17.0)
  }

  test("aggregate gauges count") {
    val expr = DataExpr.Count(Query.True)
    val dataset = createGaugeDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
    val result = aggregator.get.datapoints

    assertEquals(result.size, 1)
    assertEquals(result.head.timestamp, 0L)
    assertEquals(result.head.tags, Map("name" -> "cpu"))
    assertEquals(result.head.value, 17.0)
  }

  test("aggregate group by") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
    val result = aggregator.get.datapoints

    assertEquals(result.size, 10)
    result.foreach { d =>
      val v = d.tags("node").substring(2).toDouble
      assertEquals(d.value, v)
    }
  }

  test("aggregate gauges group by") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createGaugeDatapoints(expr, 0, 10)

    // Copy of a datapoint with a different atlas.aggr value, ensure that it gets
    // added to the others and doesn't result in a duplicate value for the key
    val d = dataset.head
    val d2 = d.copy(
      source = "test",
      tags = d.tags + ("atlas.aggr" -> "test"),
      value = 10.0
    )

    val aggregator =
      AggrDatapoint.aggregate(d2 :: dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
    val result = aggregator.get.datapoints

    assertEquals(result.size, 2)
    result.foreach { d =>
      val v = d.tags("node").substring(2).toInt
      assertEquals(d.value, if (v % 2 == 0) 18.0 else 9.0)
    }
  }

  test("aggregate group by exceeds input data points") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(5, Integer.MAX_VALUE))

    assert(aggregator.get.limitExceeded)
  }

  test("aggregate group by exceeds intermediate data points") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("node"))
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, 5))

    assert(aggregator.get.limitExceeded)
  }

  test("aggregate all") {
    val expr = DataExpr.All(Query.True)
    val dataset = createDatapoints(expr, 0, 10)
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
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
      AggrDatapoint.aggregate(dataset, settings(5, Integer.MAX_VALUE))

    assert(aggregator.get.limitExceeded)
  }

  test("aggregate with samples") {
    val expr = DataExpr.Sum(Query.True)
    val dataset = createDatapoints(expr, 0, 10).map { dp =>
      dp.copy(samples = List(List(dp.value)))
    }
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
    val result = aggregator.get.datapoints

    assertEquals(result.size, 1)
    assertEquals(result.head.timestamp, 0L)
    assertEquals(result.head.tags, Map("name" -> "cpu"))
    assertEquals(result.head.value, 45.0)
    assertEquals(result.head.samples, List(List(0.0)))
  }

  test("aggregate group by with samples") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("name", "node"))
    val dataset = createDatapoints(expr, 0, 10).map { dp =>
      dp.copy(samples = List(List(dp.value)))
    }
    val aggregator =
      AggrDatapoint.aggregate(dataset, settings(Integer.MAX_VALUE, Integer.MAX_VALUE))
    val result = aggregator.get.datapoints.sortWith(_.tags("node") > _.tags("node"))

    assertEquals(result.size, 10)
    assertEquals(result.head.timestamp, 0L)
    assertEquals(result.head.tags, Map("name" -> "cpu", "node" -> "i-00000009"))
    assertEquals(result.head.value, 9.0)
    assertEquals(result.head.samples, List(List(9.0)))
  }
}
