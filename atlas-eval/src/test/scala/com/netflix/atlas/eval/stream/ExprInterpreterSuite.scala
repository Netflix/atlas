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

import com.netflix.atlas.eval.model.ExprType
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.http.scaladsl.model.Uri

class ExprInterpreterSuite extends FunSuite {

  private val interpreter = new ExprInterpreter(ConfigFactory.load())

  test("determineExprType time series") {
    assertEquals(interpreter.determineExprType(Uri("/api/v1/graph")), ExprType.TIME_SERIES)
    assertEquals(interpreter.determineExprType(Uri("/api/v2/fetch")), ExprType.TIME_SERIES)
    assertEquals(interpreter.determineExprType(Uri("/api/v1/graph/")), ExprType.TIME_SERIES)
    assertEquals(interpreter.determineExprType(Uri("/graph")), ExprType.TIME_SERIES)
  }

  test("determineExprType events") {
    assertEquals(interpreter.determineExprType(Uri("/api/v1/events")), ExprType.EVENTS)
    assertEquals(interpreter.determineExprType(Uri("/api/v1/events/")), ExprType.EVENTS)
    assertEquals(interpreter.determineExprType(Uri("/events")), ExprType.EVENTS)
  }

  test("determineExprType trace events") {
    assertEquals(interpreter.determineExprType(Uri("/api/v1/traces")), ExprType.TRACE_EVENTS)
    assertEquals(interpreter.determineExprType(Uri("/api/v1/traces/")), ExprType.TRACE_EVENTS)
    assertEquals(interpreter.determineExprType(Uri("/traces")), ExprType.TRACE_EVENTS)
  }

  test("determineExprType trace time series") {
    assertEquals(
      interpreter.determineExprType(Uri("/api/v1/traces/graph")),
      ExprType.TRACE_TIME_SERIES
    )
    assertEquals(
      interpreter.determineExprType(Uri("/api/v1/traces/graph/")),
      ExprType.TRACE_TIME_SERIES
    )
    assertEquals(interpreter.determineExprType(Uri("/traces/graph")), ExprType.TRACE_TIME_SERIES)
  }
}
