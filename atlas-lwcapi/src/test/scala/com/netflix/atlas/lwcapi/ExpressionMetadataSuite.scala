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
package com.netflix.atlas.lwcapi

import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.netflix.atlas.json.Json
import org.scalatest.funsuite.AnyFunSuite

class ExpressionMetadataSuite extends AnyFunSuite {
  test("default frequency is applied") {
    val ret1 = ExpressionMetadata("this")
    val ret2 = ExpressionMetadata("this", ApiSettings.defaultFrequency)
    assert(ret1 === ret2)
  }

  test("default id is applied") {
    val ret1 = ExpressionMetadata("this", 5)
    assert(ret1.id.nonEmpty)
  }

  test("full params") {
    val expr = ExpressionMetadata("test", 60000, "idHere")
    assert(expr.expression === "test")
    assert(expr.frequency === 60000)
    assert(expr.id === "idHere")
  }

  test("default frequency") {
    val expr = ExpressionMetadata("test")
    assert(expr.frequency === ApiSettings.defaultFrequency)
  }

  test("computes id") {
    val expr = ExpressionMetadata("test")
    assert(expr.id === "2684d3c5cb245bd2fd6ee4ea30a500e97ace8141")
  }

  test("id computation considers frequency") {
    val exp1 = ExpressionMetadata("test", 10000)
    val exp2 = ExpressionMetadata("test", 20000)
    assert(exp1.id != exp2.id)
  }

  test("parses from json without frequency provides default") {
    val json = """{"expression": "this"}"""

    val o = Json.decode[ExpressionMetadata](json)
    assert(o.expression === "this")
    assert(o.frequency === ApiSettings.defaultFrequency)
  }

  test("parses from json with frequency, without id") {
    val json =
      """
        | {"expression": "this", "frequency": 9999}
      """.stripMargin

    val o = Json.decode[ExpressionMetadata](json)

    assert(o.expression === "this")
    assert(o.frequency === 9999)
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
    intercept[IllegalArgumentException] {
      Json.decode[ExpressionMetadata](json)
    }
  }

  test("Fails to parse from json with null expression") {
    val json = "{\"expression\": null}"
    intercept[IllegalArgumentException] {
      Json.decode[ExpressionMetadata](json)
    }
  }

  test("Fails to parse from json with missing expression") {
    val json = "{}"
    intercept[IllegalArgumentException] {
      Json.decode[ExpressionMetadata](json)
    }
  }

  test("renders as json") {
    val expected = """{"expression":"this","frequency":99000,"id":"foo"}"""
    val json = ExpressionMetadata("this", 99000, "foo").toJson
    assert(expected === json)
  }

  test("renders as json with default frequency") {
    val expected =
      "{\"expression\":\"this\",\"frequency\":60000,\"id\":\"fc3a081088771e05bdc3aa99ffd8770157dfe6ce\"}"
    val json = ExpressionMetadata("this").toJson
    assert(expected === json)
  }

  test("renders as json with frequency of 0") {
    val expected =
      "{\"expression\":\"this\",\"frequency\":60000,\"id\":\"fc3a081088771e05bdc3aa99ffd8770157dfe6ce\"}"
    val json = ExpressionMetadata("this", 0).toJson
    assert(expected === json)
  }

  test("expression is required to be non-null") {
    intercept[IllegalArgumentException] {
      ExpressionMetadata(null)
    }
  }

  test("sorts") {
    val source =
      List(ExpressionMetadata("a", 2), ExpressionMetadata("z", 1), ExpressionMetadata("c", 3))
    val expected =
      List(ExpressionMetadata("a", 2), ExpressionMetadata("c", 3), ExpressionMetadata("z", 1))
    assert(expected === source.sorted)
  }
}
