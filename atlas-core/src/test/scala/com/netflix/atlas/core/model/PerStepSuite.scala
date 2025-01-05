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

import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

class PerStepSuite extends FunSuite {

  private val interpreter = Interpreter(MathVocabulary.allWords)

  def eval(str: String, step: Long, value: Double): List[TimeSeries] = {
    val expr = interpreter.execute(str).stack match {
      case (v: TimeSeriesExpr) :: _ => v
      case _                        => throw new IllegalArgumentException("invalid expr")
    }
    val seq = new FunctionTimeSeq(DsType.Rate, step, _ => value)
    val input = List(LazyTimeSeries(Map("name" -> "test"), "test", seq))
    val context = EvalContext(0L, step * 2, step)
    expr.eval(context, input).data
  }

  test("1ms step") {
    val step = 1L
    val data = eval("name,test,:eq,:sum,:per-step", step, 4.0)
    assertEquals(data.size, 1)
    assertEquals(data.head.data.apply(step), 0.004)
  }

  test("500ms step") {
    val step = 500L
    val data = eval("name,test,:eq,:sum,:per-step", step, 4.0)
    assertEquals(data.size, 1)
    assertEquals(data.head.data.apply(step), 2.0)
  }

  test("1s step") {
    val step = 1_000L
    val data = eval("name,test,:eq,:sum,:per-step", step, 4.0)
    assertEquals(data.size, 1)
    assertEquals(data.head.data.apply(step), 4.0)
  }

  test("5s step") {
    val step = 5_000L
    val data = eval("name,test,:eq,:sum,:per-step", step, 4.0)
    assertEquals(data.size, 1)
    assertEquals(data.head.data.apply(step), 20.0)
  }

  test("1m step") {
    val step = 60_000L
    val data = eval("name,test,:eq,:sum,:per-step", step, 4.0)
    assertEquals(data.size, 1)
    assertEquals(data.head.data.apply(step), 240.0)
  }

  test("10m step") {
    val step = 600_000L
    val data = eval("name,test,:eq,:sum,:per-step", step, 4.0)
    assertEquals(data.size, 1)
    assertEquals(data.head.data.apply(step), 2400.0)
  }
}
