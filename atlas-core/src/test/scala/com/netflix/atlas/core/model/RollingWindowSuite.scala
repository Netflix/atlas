/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Strings
import munit.FunSuite

class RollingWindowSuite extends FunSuite {

  private val interpreter = Interpreter(StatefulVocabulary.allWords)

  private def parse(str: String): TimeSeriesExpr = {
    interpreter.execute(str).stack match {
      case (v: TimeSeriesExpr) :: Nil => v
      case _                          => throw new IllegalArgumentException(str)
    }
  }

  private def eval(expr: TimeSeriesExpr, step: Long, values: Double*): List[Double] = {
    val input = TimeSeries(
      Map("name" -> "test"),
      new ArrayTimeSeq(DsType.Gauge, 0L, step, values.toArray)
    )
    val context = EvalContext(0L, step * values.length, step)
    val result = expr.eval(context, List(input)).data.head
    values.indices.map(i => result.data(i * step)).toList
  }

  test("steps window is the same regardless of the step size") {
    val w = RollingWindow.Steps(3)
    assertEquals(w.period(1000L), 3)
    assertEquals(w.period(60000L), 3)
  }

  test("duration window is rounded down to the step boundary") {
    val w = RollingWindow.Time(Duration.ofMinutes(5))
    assertEquals(w.period(60000L), 5)
    assertEquals(w.period(120000L), 2) // 5m window with a 2m step is a 4m window
  }

  test("duration window is a single datapoint if the step is larger") {
    val w = RollingWindow.Time(Duration.ofMinutes(1))
    assertEquals(w.period(60000L), 1)
    assertEquals(w.period(300000L), 1)
  }

  test("duration window does not overflow for absurd durations") {
    val w = RollingWindow.Time(Duration.ofDays(365L * 1000_000_000L))
    assertEquals(w.period(60000L), Int.MaxValue)
  }

  test("duration window does not overflow at the max seconds boundary") {
    // Duration.toMillis overflows in the add, not just the multiply, so a duration with the
    // maximum number of whole seconds plus a fractional part must be clamped as well. This
    // form is reachable from a user expression, so it must not fail with an ArithmeticException.
    val d = Strings.parseDuration("PT9223372036854775.999S")
    assertEquals(RollingWindow.Time(d).period(60000L), Int.MaxValue)
    assertEquals(
      ModelDataTypes.RollingWindowType.unapply("PT9223372036854775.999S"),
      Some(RollingWindow.Time(d))
    )
  }

  test("datapoints for a duration smaller than or equal to the step") {
    assertEquals(RollingWindow.datapoints(Duration.ofSeconds(30), 60000L), 0)
    assertEquals(RollingWindow.datapoints(Duration.ofMinutes(1), 60000L), 1)
    assertEquals(RollingWindow.datapoints(Duration.ZERO, 60000L), 0)
    assertEquals(RollingWindow.datapoints(Duration.ofMinutes(-5), 60000L), -5)
  }

  test("window sizes must be positive") {
    intercept[IllegalArgumentException](RollingWindow.Steps(0))
    intercept[IllegalArgumentException](RollingWindow.Time(Duration.ZERO))
    intercept[IllegalArgumentException](RollingWindow.Time(Duration.ofMinutes(-1)))
  }

  test("window type extraction") {
    val t = ModelDataTypes.RollingWindowType
    assertEquals(t.unapply("3"), Some(RollingWindow.Steps(3)))
    assertEquals(t.unapply("5m"), Some(RollingWindow.Time(Duration.ofMinutes(5))))
    assertEquals(t.unapply("PT5M"), Some(RollingWindow.Time(Duration.ofMinutes(5))))
    assertEquals(t.unapply(Duration.ofMinutes(5)), Some(RollingWindow.Time(Duration.ofMinutes(5))))
    assertEquals(t.unapply("0"), None)
    assertEquals(t.unapply("-1"), None)
    assertEquals(t.unapply("0s"), None)
    assertEquals(t.unapply("foo"), None)
  }

  test("non-positive window is not a valid expression") {
    intercept[IllegalStateException](parse("name,test,:eq,:sum,0,:rolling-max"))
    intercept[IllegalStateException](parse("name,test,:eq,:sum,0,:delay"))
    intercept[IllegalStateException](parse("name,test,:eq,:sum,-1,:delay"))
  }

  test("duration window covers the same amount of time at different steps") {
    val expr = parse("name,test,:eq,:sum,1m,:rolling-max")

    // 20s step, 1m window, so the max is over the last three datapoints
    assertEquals(
      eval(expr, 20000L, 1.0, 5.0, 2.0, 3.0, 4.0),
      List(1.0, 5.0, 5.0, 5.0, 4.0)
    )

    // 30s step, 1m window, so the max is over the last two datapoints
    assertEquals(
      eval(expr, 30000L, 1.0, 5.0, 2.0, 3.0, 4.0),
      List(1.0, 5.0, 5.0, 3.0, 4.0)
    )

    // 1m step, the window is a single datapoint and the input passes through
    assertEquals(
      eval(expr, 60000L, 1.0, 5.0, 2.0, 3.0, 4.0),
      List(1.0, 5.0, 2.0, 3.0, 4.0)
    )
  }

  test("datapoint window covers less time as the step increases") {
    val expr = parse("name,test,:eq,:sum,3,:rolling-max")
    assertEquals(
      eval(expr, 20000L, 1.0, 5.0, 2.0, 3.0, 4.0),
      List(1.0, 5.0, 5.0, 5.0, 4.0)
    )
    assertEquals(
      eval(expr, 60000L, 1.0, 5.0, 2.0, 3.0, 4.0),
      List(1.0, 5.0, 5.0, 5.0, 4.0)
    )
  }

  test("rolling-count with a window smaller than the step counts occurrences") {
    // A single datapoint window is not a passthrough for :rolling-count, the output is still
    // the number of non-zero values within the window.
    val expr = parse("name,test,:eq,:sum,1m,:rolling-count")
    assertEquals(
      eval(expr, 300000L, 5.0, 0.0, 7.0),
      List(1.0, 0.0, 1.0)
    )
  }

  test("rolling-mean emits NaN if the window is smaller than minNumValues") {
    // The window size depends on the step, so the same expression must not start failing when
    // the graph is zoomed out to a step where the window is a single datapoint. NaN is compared
    // via toString because NaN is not equal to itself for numeric comparisons.
    val expr = parse("name,test,:eq,:sum,1m,3,:rolling-mean")

    // 20s step, 1m window, so there are enough datapoints once the buffer is full
    assertEquals(
      eval(expr, 20000L, 1.0, 2.0, 3.0, 4.0).mkString(","),
      "NaN,NaN,2.0,3.0"
    )

    // 1m step, the window is a single datapoint and can never have 3 values
    assertEquals(
      eval(expr, 60000L, 1.0, 2.0, 3.0, 4.0).mkString(","),
      "NaN,NaN,NaN,NaN"
    )
  }

  test("delay shifts by the same amount of time at different steps") {
    val expr = parse("name,test,:eq,:sum,1m,:delay")

    // 30s step, 1m delay, so the input is shifted by two datapoints
    assertEquals(
      eval(expr, 30000L, 1.0, 2.0, 3.0, 4.0).mkString(","),
      "NaN,NaN,1.0,2.0"
    )

    // 1m step, the delay is a single datapoint
    assertEquals(
      eval(expr, 60000L, 1.0, 2.0, 3.0, 4.0).mkString(","),
      "NaN,1.0,2.0,3.0"
    )
  }

  test("delay is a single step if the step is larger than the window") {
    // The shift is a single datapoint rather than nothing, so at a 5m step a 1m delay shifts
    // the input by 5m. This is the one case where the delay is not the requested amount of
    // time, hence a separate test from the fixed duration cases above.
    val expr = parse("name,test,:eq,:sum,1m,:delay")
    assertEquals(
      eval(expr, 300000L, 1.0, 2.0, 3.0, 4.0).mkString(","),
      "NaN,1.0,2.0,3.0"
    )
  }

  test("delay with a datapoint window shifts by a fixed number of intervals") {
    val expr = parse("name,test,:eq,:sum,2,:delay")
    assertEquals(
      eval(expr, 30000L, 1.0, 2.0, 3.0, 4.0).mkString(","),
      "NaN,NaN,1.0,2.0"
    )
    assertEquals(
      eval(expr, 300000L, 1.0, 2.0, 3.0, 4.0).mkString(","),
      "NaN,NaN,1.0,2.0"
    )
  }

  test("round trip toString") {
    val exprs = List(
      "name,test,:eq,:sum,3,:delay",
      "name,test,:eq,:sum,PT1M,:delay",
      "name,test,:eq,:sum,3,:rolling-count",
      "name,test,:eq,:sum,PT1M,:rolling-count",
      "name,test,:eq,:sum,3,:rolling-min",
      "name,test,:eq,:sum,PT1M,:rolling-min",
      "name,test,:eq,:sum,3,:rolling-max",
      "name,test,:eq,:sum,PT1M,:rolling-max",
      "name,test,:eq,:sum,3,2,:rolling-mean",
      "name,test,:eq,:sum,PT1M,2,:rolling-mean",
      "name,test,:eq,:sum,3,:rolling-sum",
      "name,test,:eq,:sum,PT1M,:rolling-sum"
    )
    exprs.foreach { str =>
      assertEquals(parse(str).toString, str)
    }
  }

  test("duration is normalized to the ISO form") {
    assertEquals(
      parse("name,test,:eq,:sum,1m,:rolling-max").toString,
      "name,test,:eq,:sum,PT1M,:rolling-max"
    )
  }
}
