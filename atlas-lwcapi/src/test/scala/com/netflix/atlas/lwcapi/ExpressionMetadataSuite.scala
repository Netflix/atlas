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
package com.netflix.atlas.lwcapi

import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.fasterxml.jackson.databind.exc.ValueInstantiationException
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.json.Json
import munit.FunSuite

class ExpressionMetadataSuite extends FunSuite {

  test("default frequency is applied") {
    val ret1 = ExpressionMetadata("this")
    val ret2 = ExpressionMetadata("this", ExprType.TIME_SERIES, ApiSettings.defaultStep)
    assertEquals(ret1, ret2)
  }

  test("default id is applied") {
    val ret1 = ExpressionMetadata("this", ExprType.TIME_SERIES, 5)
    assert(ret1.id.nonEmpty)
  }

  test("full params") {
    val expr = ExpressionMetadata("test", ExprType.TIME_SERIES, 60000, "idHere")
    assertEquals(expr.expression, "test")
    assertEquals(expr.frequency, 60000L)
    assertEquals(expr.id, "idHere")
  }

  test("default frequency") {
    val expr = ExpressionMetadata("test")
    assertEquals(expr.frequency, ApiSettings.defaultStep)
  }

  test("computes id") {
    val expr = ExpressionMetadata("test")
    assertEquals(expr.id, "59ec3895749cc3fb279d41dabc1d4943361de999")
  }

  test("id computation considers frequency") {
    val exp1 = ExpressionMetadata("test", ExprType.TIME_SERIES, 10000)
    val exp2 = ExpressionMetadata("test", ExprType.TIME_SERIES, 20000)
    assert(exp1.id != exp2.id)
  }

  test("parses from json without frequency provides default") {
    val json = """{"expression": "this"}"""

    val o = Json.decode[ExpressionMetadata](json)
    assertEquals(o.expression, "this")
    assertEquals(o.frequency, ApiSettings.defaultStep)
  }

  test("parses from json with frequency, without id") {
    val json =
      """
        | {"expression": "this", "frequency": 9999}
      """.stripMargin

    val o = Json.decode[ExpressionMetadata](json)

    assertEquals(o.expression, "this")
    assertEquals(o.frequency, 9999L)
    assert(o.id.isEmpty)
  }

  test("fails to parse from json with frequency non-integer") {
    val json = """{"expression": "this", "frequency": "that"}"""
    intercept[InvalidFormatException] {
      Json.decode[ExpressionMetadata](json)
    }
  }

  test("Fails to parse from json with empty expression") {
    val json = "{\"expression\": \"\"}"
    intercept[ValueInstantiationException] {
      Json.decode[ExpressionMetadata](json)
    }
  }

  test("Fails to parse from json with null expression") {
    val json = "{\"expression\": null}"
    intercept[ValueInstantiationException] {
      Json.decode[ExpressionMetadata](json)
    }
  }

  test("Fails to parse from json with missing expression") {
    val json = "{}"
    intercept[ValueInstantiationException] {
      Json.decode[ExpressionMetadata](json)
    }
  }

  test("renders as json") {
    val expected = """{"expression":"this","exprType":"TIME_SERIES","frequency":99000,"id":"foo"}"""
    val json = ExpressionMetadata("this", ExprType.TIME_SERIES, 99000, "foo").toJson
    assertEquals(expected, json)
  }

  test("renders as json with default frequency") {
    val expected =
      "{\"expression\":\"this\",\"exprType\":\"TIME_SERIES\",\"frequency\":60000,\"id\":\"8189ff24a1e801c924689aeb0490d5d840f23582\"}"
    val json = ExpressionMetadata("this").toJson
    assertEquals(expected, json)
  }

  test("renders as json with frequency of 0") {
    val expected =
      "{\"expression\":\"this\",\"exprType\":\"TIME_SERIES\",\"frequency\":60000,\"id\":\"8189ff24a1e801c924689aeb0490d5d840f23582\"}"
    val json = ExpressionMetadata("this", ExprType.TIME_SERIES, 0).toJson
    assertEquals(expected, json)
  }

  test("expression is required to be non-null") {
    intercept[IllegalArgumentException] {
      ExpressionMetadata(null)
    }
  }

  test("sorts") {
    val source = List(
      ExpressionMetadata("a", ExprType.TIME_SERIES, 2),
      ExpressionMetadata("z", ExprType.TIME_SERIES, 1),
      ExpressionMetadata("c", ExprType.TIME_SERIES, 3)
    )
    val expected = List(
      ExpressionMetadata("a", ExprType.TIME_SERIES, 2),
      ExpressionMetadata("c", ExprType.TIME_SERIES, 3),
      ExpressionMetadata("z", ExprType.TIME_SERIES, 1)
    )
    assertEquals(expected, source.sorted)
  }
}
