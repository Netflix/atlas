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
package com.netflix.atlas.eval.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.spectator.api.DefaultRegistry
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class TimeGroupedSuite extends FunSuite {

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val materializer = ActorMaterializer()

  private val registry = new DefaultRegistry()

  private val context = TestContext.createContext(materializer, registry = registry)

  private val step = 10

  private def result(future: Future[List[TimeGroup]]): List[TimeGroup] = {
    Await
      .result(future, Duration.Inf)
      .reverse
      .map(g => g.copy(values = g.values.mapValues(_.sortWith(_.value < _.value))))
  }

  private def run(data: List[AggrDatapoint]): List[TimeGroup] = {
    val future = Source(data)
      .via(new TimeGrouped(context, 10))
      .runFold(List.empty[TimeGroup])((acc, g) => g :: acc)
    result(future)
  }

  private def timeGroup(t: Long, vs: List[AggrDatapoint]): TimeGroup = {
    val expr = DataExpr.All(Query.True)
    if (vs.isEmpty)
      TimeGroup(t, step, Map.empty)
    else
      TimeGroup(t, step, Map(expr -> vs))
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
    assert(
      groups === List(
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
    assert(
      groups === List(
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

    assert(
      groups === List(
        timeGroup(10, List(datapoint(10, 1), datapoint(10, 2), datapoint(10, 3))),
        timeGroup(20, List(datapoint(20, 1))),
        timeGroup(30, List(datapoint(30, 1), datapoint(30, 2)))
      )
    )

    assert(before._1 + 6 === after._1) // 6 buffered messages
    assert(before._2 + 1 === after._2) // 1 dropped message
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

    assert(
      groups === List(
        timeGroup(10, List(datapoint(10, 2), datapoint(10, 3))),
        timeGroup(20, List(datapoint(20, 1))),
        timeGroup(30, List(datapoint(30, 1), datapoint(30, 2)))
      )
    )

    assert(before._1 + 5 === after._1) // 5 buffered messages
    assert(before._2 + 2 === after._2) // 2 dropped message
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
    assert(
      groups === List(
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
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, n * (n - 1) / 2)

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, Map(expr -> List(expected)))))
  }

  test("simple aggregate: min") {
    val n = 10000
    val expr = DataExpr.Min(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, 0)

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, Map(expr -> List(expected)))))
  }

  test("simple aggregate: max") {
    val n = 10000
    val expr = DataExpr.Max(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, n - 1)

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, Map(expr -> List(expected)))))
  }

  test("simple aggregate: count") {
    val n = 10000
    val expr = DataExpr.Count(Query.True)
    val data = (0 until n).toList.map { i =>
      AggrDatapoint(10, 10, expr, "test", Map.empty, i)
    }
    val expected = AggrDatapoint(10, 10, expr, "test", Map.empty, n * (n - 1) / 2)

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, Map(expr -> List(expected)))))
  }

  test("group by aggregate: sum") {
    val n = 5000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Sum(Query.True), List("category"))
    val data = (0 until 2 * n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> List(
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), n * (n - 1)),
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), n * n)
      )
    )

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, expected)))
  }

  test("group by aggregate: min") {
    val n = 10000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Min(Query.True), List("category"))
    val data = (0 until n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> List(
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), 0),
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), 1)
      )
    )

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, expected)))
  }

  test("group by aggregate: max") {
    val n = 10000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Max(Query.True), List("category"))
    val data = (0 until n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> List(
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), n - 2),
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), n - 1)
      )
    )

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, expected)))
  }

  test("group by aggregate: count") {
    val n = 5000
    val expr: DataExpr = DataExpr.GroupBy(DataExpr.Count(Query.True), List("category"))
    val data = (0 until 2 * n).toList.map { i =>
      val category = if (i % 2 == 0) "even" else "odd"
      AggrDatapoint(10, 10, expr, "test", Map("category" -> category), i)
    }
    val expected = Map(
      expr -> List(
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "even"), n * (n - 1)),
        AggrDatapoint(10, 10, expr, "test", Map("category" -> "odd"), n * n)
      )
    )

    val groups = run(data)
    assert(groups === List(TimeGroup(10, step, expected)))
  }
}
