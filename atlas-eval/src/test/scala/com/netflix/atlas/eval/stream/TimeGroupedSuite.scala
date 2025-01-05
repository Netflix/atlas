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
package com.netflix.atlas.eval.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.AggrValuesInfo
import com.netflix.atlas.eval.model.DatapointsTuple
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.spectator.api.DefaultRegistry
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class TimeGroupedSuite extends FunSuite {

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val materializer: Materializer = Materializer(system)

  private val registry = new DefaultRegistry()

  private val context = TestContext.createContext(materializer, registry = registry)

  private val step = 10

  private def result(future: Future[List[TimeGroup]]): List[TimeGroup] = {
    Await
      .result(future, Duration.Inf)
      .reverse
      .map(g =>
        g.copy(dataExprValues =
          g.dataExprValues
            .map(t => t._1 -> t._2.copy(values = t._2.values.sortWith(_.value < _.value)))
        )
      )
  }

  private def run(data: List[AggrDatapoint]): List[TimeGroup] = {
    val future = Source
      .single(DatapointsTuple(data))
      .via(new TimeGrouped(context))
      .flatMapConcat(t => Source(t.groups))
      .runFold(List.empty[TimeGroup])((acc, g) => g :: acc)
    result(future)
  }

  private def timeGroup(t: Long, vs: List[AggrDatapoint]): TimeGroup = {
    val expr = DataExpr.All(Query.True)
    if (vs.isEmpty)
      TimeGroup(t, step, Map.empty)
    else
      TimeGroup(t, step, Map(expr -> AggrValuesInfo(vs, vs.size)))
  }

  private def datapoint(t: Long, v: Int): AggrDatapoint = {
    val expr = DataExpr.All(Query.True)
    AggrDatapoint(t, step, expr, "test", Map.empty, v)
  }

  test("in order list") {
    val data =
      List(
        datapoint(10, 1),
        datapoint(10, 2),
        datapoint(10, 3),
        datapoint(20, 1),
        datapoint(30, 1),
        datapoint(30, 2)
      )

    val groups = run(data)
    assertEquals(
      groups,
      List(
        timeGroup(10, List(datapoint(10, 1), datapoint(10, 2), datapoint(10, 3))),
        timeGroup(20, List(datapoint(20, 1))),
        timeGroup(30, List(datapoint(30, 1), datapoint(30, 2)))
      )
    )
  }

  test("out of order list") {
    val data =
      List(
        datapoint(20, 1),
        datapoint(10, 2),
        datapoint(10, 3),
        datapoint(10, 1),
        datapoint(30, 1),
        datapoint(30, 2)
      )

    val groups = run(data)
    assertEquals(
      groups,
      List(
        timeGroup(10, List(datapoint(10, 1), datapoint(10, 2), datapoint(10, 3))),
        timeGroup(20, List(datapoint(20, 1))),
        timeGroup(30, List(datapoint(30, 1), datapoint(30, 2)))
      )
    )
  }

  private def count(id: String): Long = {
    registry.counter("atlas.eval.datapoints", "id", id).count()
  }

  private def counts: (Long, Long) = {
    count("buffered") -> (count("dropped-old") + count("dropped-future"))
  }

  test("drop events for an expression that exceed the number of input datapoints limit") {
    val n = 60000
    val expr = DataExpr.Max(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }

    val before = count("dropped-datapoints-limit-exceeded")
    val groups = run(data)
    val after = count("dropped-datapoints-limit-exceeded")

    assertEquals(groups, List(timeGroup(10, Nil)))
    assertEquals(before, 0L)
    assertEquals(after, 10000L)
  }

  test("late events dropped") {
    val data = List(
      datapoint(20, 1),
      datapoint(10, 2),
      datapoint(10, 3),
      datapoint(10, 1),
      datapoint(30, 1),
      datapoint(30, 2),
      datapoint(10, 4) // Dropped, came in late and out of window
    )

    val before = counts
    val groups = run(data)
    val after = counts

    assertEquals(
      groups,
      List(
        timeGroup(10, List(datapoint(10, 1), datapoint(10, 2), datapoint(10, 3))),
        timeGroup(20, List(datapoint(20, 1))),
        timeGroup(30, List(datapoint(30, 1), datapoint(30, 2)))
      )
    )

    assertEquals(before._1 + 6, after._1) // 6 buffered messages
    assertEquals(before._2 + 1, after._2) // 1 dropped message
  }

  test("future events dropped") {
    val future = System.currentTimeMillis() + 60 * 60 * 1000
    val data = List(
      datapoint(20, 1),
      datapoint(10, 2),
      datapoint(10, 3),
      datapoint(future + 10, 1), // Dropped, timestamp in the future
      datapoint(30, 1),
      datapoint(30, 2),
      datapoint(10, 4) // Dropped, came in late and out of window
    )

    val before = counts
    val groups = run(data)
    val after = counts

    assertEquals(
      groups,
      List(
        timeGroup(10, List(datapoint(10, 2), datapoint(10, 3))),
        timeGroup(20, List(datapoint(20, 1))),
        timeGroup(30, List(datapoint(30, 1), datapoint(30, 2)))
      )
    )

    assertEquals(before._1 + 5, after._1) // 5 buffered messages
    assertEquals(before._2 + 2, after._2) // 2 dropped message
  }

  test("heartbeat will flush if no data for an interval") {
    val data =
      List(
        AggrDatapoint.heartbeat(10, step),
        datapoint(10, 1),
        datapoint(10, 2),
        datapoint(10, 3),
        AggrDatapoint.heartbeat(20, step),
        datapoint(30, 1),
        datapoint(30, 2),
        AggrDatapoint.heartbeat(30, step)
      )

    val groups = run(data)
    assertEquals(
      groups,
      List(
        timeGroup(10, List(datapoint(10, 1), datapoint(10, 2), datapoint(10, 3))),
        timeGroup(20, Nil),
        timeGroup(30, List(datapoint(30, 1), datapoint(30, 2)))
      )
    )
  }

  test("simple aggregate: sum") {
    val n = 10000
    val expr = DataExpr.Sum(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, n.toDouble * (n - 1) / 2)

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, Map(expr -> AggrValuesInfo(List(expected), n)))))
  }

  test("simple aggregate: min") {
    val n = 10000
    val expr = DataExpr.Min(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, 0)

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, Map(expr -> AggrValuesInfo(List(expected), n)))))
  }

  test("simple aggregate: max") {
    val n = 10000
    val expr = DataExpr.Max(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, n - 1)

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, Map(expr -> AggrValuesInfo(List(expected), n)))))
  }

  test("simple aggregate: count") {
    val n = 10000
    val expr = DataExpr.Count(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, n.toDouble * (n - 1) / 2)

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, Map(expr -> AggrValuesInfo(List(expected), n)))))
  }

  test("group by aggregate: sum") {
    val n = 5000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("category"))
    val data = (0 until 2 * n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> AggrValuesInfo(
        List(
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), n * (n - 1)),
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), n * n)
        ),
        2 * n
      )
    )

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, expected)))
  }

  test("group by aggregate: min") {
    val n = 10000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Min(Query.True), List("category"))
    val data = (0 until n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> AggrValuesInfo(
        List(
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), 0),
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), 1)
        ),
        n
      )
    )

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, expected)))
  }

  test("group by aggregate: max") {
    val n = 10000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Max(Query.True), List("category"))
    val data = (0 until n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> AggrValuesInfo(
        List(
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), n - 2),
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), n - 1)
        ),
        n
      )
    )

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, expected)))
  }

  test("group by aggregate: count") {
    val n = 5000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Count(Query.True), List("category"))
    val data = (0 until 2 * n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> AggrValuesInfo(
        List(
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), n * (n - 1)),
          AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), n * n)
        ),
        2 * n
      )
    )

    val groups = run(data)
    assertEquals(groups, List(TimeGroup(10, step, expected)))
  }
}
