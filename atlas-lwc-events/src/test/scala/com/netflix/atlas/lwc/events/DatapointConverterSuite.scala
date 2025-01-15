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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.spectator.api.ManualClock
import munit.FunSuite

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

class DatapointConverterSuite extends FunSuite {

  private val clock = new ManualClock()
  private val step = 5_000L

  override def beforeEach(context: BeforeEach): Unit = {
    clock.setWallTime(0L)
    clock.setMonotonicTime(0L)
  }

  test("toDouble") {
    assertEquals(DatapointConverter.toDouble(null, -1.0), -1.0)
    assertEquals(DatapointConverter.toDouble(false, -1.0), 0.0)
    assertEquals(DatapointConverter.toDouble(true, -1.0), 1.0)
    assertEquals(DatapointConverter.toDouble(42, -1.0), 42.0)
    assertEquals(DatapointConverter.toDouble(42L, -1.0), 42.0)
    assertEquals(DatapointConverter.toDouble(42.5f, -1.0), 42.5)
    assertEquals(DatapointConverter.toDouble(42.5, -1.0), 42.5)
    assertEquals(DatapointConverter.toDouble(new AtomicLong(42), -1.0), 42.0)
    assertEquals(DatapointConverter.toDouble("42", -1.0), 42.0)
    assertEquals(DatapointConverter.toDouble("42e3", -1.0), 42e3)
    assertEquals(DatapointConverter.toDouble(Duration.ofMillis(42131), -1.0), 42.131)
    assertEquals(DatapointConverter.toDouble(Duration.ofSeconds(42131), -1.0), 42131.0)
    assertEquals(DatapointConverter.toDouble(Duration.ofMinutes(2), -1.0), 120.0)
    assertEquals(DatapointConverter.toDouble(null, Duration.ofMillis(42)), 0.042)
    assert(DatapointConverter.toDouble("foo", -1.0).isNaN)
  }

  test("counter - sum") {
    val expr = DataExpr.Sum(Query.True)
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("value" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Map.empty, step, 1.0))
  }

  test("counter - sum with sample") {
    val expr = DataExpr.Sum(Query.True)
    val events = List.newBuilder[LwcEvent]
    val converter = DatapointConverter(
      "id",
      expr.toString,
      expr,
      clock,
      step,
      Some(e => List(e.extractValue("value"))),
      (_, e) => events.addOne(e)
    )
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("value" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Map.empty, step, 1.0, List(List(0))))
  }

  test("counter - sum custom value") {
    val expr = DataExpr.Sum(Query.Equal("value", "responseSize"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Map("value" -> "responseSize"), step, 2.0))
  }

  test("counter - count") {
    val expr = DataExpr.Count(Query.Equal("value", "responseSize"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Map("value" -> "responseSize"), step, 5.0))
  }

  test("counter - max custom value") {
    val expr = DataExpr.Max(Query.Equal("value", "responseSize"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Map("value" -> "responseSize"), step, 4.0))
  }

  test("counter - max negative value") {
    val expr = DataExpr.Max(Query.Equal("value", "responseSize"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> -(i + 10)))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Map("value" -> "responseSize"), step, -10.0))
  }

  test("counter - min negative value") {
    val expr = DataExpr.Min(Query.Equal("value", "responseSize"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> -i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Map("value" -> "responseSize"), step, -4.0))
  }

  private def stat(name: String): Query = {
    Query.Equal("statistic", name)
  }

  test("timer - sum of totalTime") {
    val expr = DataExpr.Sum(Query.Equal("value", "latency").and(stat("totalTime")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("latency" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 2.0))
  }

  test("timer - sum of count") {
    val expr = DataExpr.Sum(Query.Equal("value", "latency").and(stat("count")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("latency" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 1.0))
  }

  test("timer - sum of totalOfSquares") {
    val expr = DataExpr.Sum(Query.Equal("value", "latency").and(stat("totalOfSquares")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("latency" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 6.0))
  }

  test("timer - dist-max") {
    val expr = DataExpr.Max(Query.Equal("value", "latency").and(stat("max")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("latency" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 4.0))
  }

  test("timer - dist-min") {
    val expr = DataExpr.Min(Query.Equal("value", "latency").and(stat("max")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("latency" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 0.0))
  }

  test("timer - percentile") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("value", "latency")), List("percentile"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("latency" -> Duration.ofMillis(i)))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 4)
    assertEquals(results.head.value, 0.2, 1e-12)
  }

  test("dist - sum of totalAmount") {
    val expr = DataExpr.Sum(Query.Equal("value", "responseSize").and(stat("totalAmount")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 2.0))
  }

  test("dist - sum of total either") {
    val stat = Query.In("statistic", List("totalTime", "totalAmount"))
    val expr = DataExpr.Sum(Query.Equal("value", "responseSize").and(stat))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 2.0))
  }

  test("dist - sum of count") {
    val expr = DataExpr.Sum(Query.Equal("value", "responseSize").and(stat("count")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 1.0))
  }

  test("dist - sum of totalOfSquares") {
    val expr = DataExpr.Sum(Query.Equal("value", "responseSize").and(stat("totalOfSquares")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 6.0))
  }

  test("dist - dist-max") {
    val expr = DataExpr.Max(Query.Equal("value", "responseSize").and(stat("max")))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head, DatapointEvent("id", Query.tags(expr.query), step, 4.0))
  }

  test("dist - percentile") {
    val expr = DataExpr.GroupBy(
      DataExpr.Sum(Query.Equal("value", "responseSize")),
      List("percentile")
    )
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      val event = LwcEvent(Map("responseSize" -> i))
      converter.update(event)
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 4)
    assertEquals(results.head.value, 0.2, 1e-12)
  }

  test("groupBy - sum") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("value", "responseSize")), List("app"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))
    (0 until 5).foreach { i =>
      converter.update(LwcEvent(Map("responseSize" -> i, "app" -> "a")))
      converter.update(LwcEvent(Map("responseSize" -> i * 2, "app" -> "b")))
      converter.update(LwcEvent(Map("responseSize" -> i * 3, "app" -> "c")))
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 3)
    results.foreach { event =>
      val dp = event.asInstanceOf[DatapointEvent]
      assert(dp.tags.contains("app"))
      dp.tags("app") match {
        case "a" => assertEqualsDouble(dp.value, 2.0, 1e-12)
        case "b" => assertEqualsDouble(dp.value, 4.0, 1e-12)
        case "c" => assertEqualsDouble(dp.value, 6.0, 1e-12)
      }
    }
  }

  test("groupBy - sum limit exceeded") {
    val expr =
      DataExpr.GroupBy(DataExpr.Sum(Query.Equal("value", "responseSize")), List("responseSize"))
    val events = List.newBuilder[LwcEvent]
    val converter =
      DatapointConverter("id", expr.toString, expr, clock, step, None, (_, e) => events.addOne(e))

    (0 until 10_000).foreach { i =>
      converter.update(LwcEvent(Map("responseSize" -> i.toString, "app" -> "a")))
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    var results = events.result()
    assertEquals(results.size, 10_000)
    events.clear()

    (0 until 100_000).foreach { i =>
      converter.update(LwcEvent(Map("responseSize" -> i.toString, "app" -> "a")))
    }
    clock.setWallTime(step * 2 + 1)
    converter.flush(clock.wallTime())
    results = events.result()
    assertEquals(results.size, 10_000)
  }

  test("groupBy - sum with sample") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("value", "responseSize")), List("app"))
    val events = List.newBuilder[LwcEvent]
    val converter = DatapointConverter(
      "id",
      expr.toString,
      expr,
      clock,
      step,
      Some(e => List(e.extractValue("app"))),
      (_, e) => events.addOne(e)
    )
    (0 until 5).foreach { i =>
      converter.update(LwcEvent(Map("responseSize" -> i, "app" -> "a")))
      converter.update(LwcEvent(Map("responseSize" -> i * 2, "app" -> "b")))
      converter.update(LwcEvent(Map("responseSize" -> i * 3, "app" -> "c")))
    }
    clock.setWallTime(step + 1)
    converter.flush(clock.wallTime())
    val results = events.result()
    assertEquals(results.size, 3)
    results.foreach { event =>
      val dp = event.asInstanceOf[DatapointEvent]
      assert(dp.tags.contains("app"))
      dp.tags("app") match {
        case "a" =>
          assertEqualsDouble(dp.value, 2.0, 1e-12)
          assertEquals(dp.samples, List(List("a")))
        case "b" =>
          assertEqualsDouble(dp.value, 4.0, 1e-12)
          assertEquals(dp.samples, List(List("b")))
        case "c" =>
          assertEqualsDouble(dp.value, 6.0, 1e-12)
          assertEquals(dp.samples, List(List("c")))
      }
    }
  }

  test("groupBy - sum with sample ordering") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("value", "responseSize")), List("app"))
    val events = List.newBuilder[LwcEvent]
    val converter = DatapointConverter(
      "id",
      expr.toString,
      expr,
      clock,
      step,
      Some(e => List(e.extractValue("app"))),
      (_, e) => events.addOne(e)
    )
    converter.update(LwcEvent(Map("responseSize" -> 100, "app" -> "a")))
    clock.setWallTime(step + 1)

    // Update after passing step boundary and before flush
    converter.update(LwcEvent(Map("responseSize" -> 100, "app" -> "a")))
    converter.flush(clock.wallTime())
    var results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head.asInstanceOf[DatapointEvent].samples, List(List("a")))

    // Flush again with no update
    clock.setWallTime(step * 2 + 1)
    events.clear()
    converter.flush(clock.wallTime())
    results = events.result()
    assertEquals(results.size, 1)
    assertEquals(results.head.asInstanceOf[DatapointEvent].samples, List(List("a")))
  }
}
