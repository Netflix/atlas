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
package com.netflix.atlas.eval.model

import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.json.Json
import org.scalatest.funsuite.AnyFunSuite

class LwcMessagesSuite extends AnyFunSuite {

  private val step = 60000

  test("data expr, decode with legacy frequency field") {
    val json = """{"id":"1234","expression":"name,cpu,:eq,:sum","frequency":10}"""
    val actual = Json.decode[LwcDataExpr](json)
    val expected = LwcDataExpr("1234", "name,cpu,:eq,:sum", 10)
    assert(actual === expected)
  }

  test("subscription info") {
    val expr = "name,cpu,:eq,:avg"
    val sum = "name,cpu,:eq,:sum"
    val count = "name,cpu,:eq,:count"
    val dataExprs = List(LwcDataExpr("a", sum, step), LwcDataExpr("b", count, step))
    val expected = LwcSubscription(expr, dataExprs)
    val actual = LwcMessages.parse(Json.encode(expected))
    assert(actual === expected)
  }

  test("datapoint") {
    val expected = LwcDatapoint(step, "a", Map("foo" -> "bar"), 42.0)
    val actual = LwcMessages.parse(Json.encode(expected))
    assert(actual === expected)
  }

  test("datapoint, custom encode") {
    val expected = LwcDatapoint(step, "a", Map("foo" -> "bar"), 42.0)
    val actual = LwcMessages.parse(expected.toJson)
    assert(actual === expected)
  }

  test("diagnostic message") {
    val expected = DiagnosticMessage.error("something bad happened")
    val actual = LwcMessages.parse(Json.encode(expected))
    assert(actual === expected)
  }

  test("diagnostic message for a particular expression") {
    val expected = LwcDiagnosticMessage("abc", DiagnosticMessage.error("something bad happened"))
    val actual = LwcMessages.parse(Json.encode(expected))
    assert(actual === expected)
  }

  test("heartbeat") {
    val expected = LwcHeartbeat(1234567890L, 10L)
    val actual = LwcMessages.parse(Json.encode(expected))
    assert(actual === expected)
  }

  test("heartbeat not on step boundary") {
    intercept[IllegalArgumentException] {
      LwcHeartbeat(1234567891L, 10L)
    }
  }
}
