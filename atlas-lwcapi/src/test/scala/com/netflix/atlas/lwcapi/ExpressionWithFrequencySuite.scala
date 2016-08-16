/*
 * Copyright 2014-2016 Netflix, Inc.
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

import com.fasterxml.jackson.core.JsonProcessingException
import com.netflix.atlas.json.Json
import org.scalatest.FunSuite

class ExpressionWithFrequencySuite extends FunSuite {
  test("full params") {
    val expr = ExpressionWithFrequency("test", 60000)
    assert(expr.frequency === 60000)
  }

  test("default frequency") {
    val expr = ExpressionWithFrequency("test", 0)
    assert(expr.frequency === 60000)
  }

  test("parses from json without frequency provides default") {
    val json =
      """
        | {"expression": "this"}
      """.stripMargin

    val o = Json.decode[ExpressionWithFrequency](json)
    assert(o.expression === "this")
    assert(o.frequency === 60000)
  }

  test("parses from json with frequency") {
    val json =
      """
        | {"expression": "this", "frequency": 9999}
      """.stripMargin

    val o = Json.decode[ExpressionWithFrequency](json)
    assert(o.expression === "this")
    assert(o.frequency === 9999)
  }

  test("fails to parse from json with frequency non-integer") {
    val json =
      """
        | {"expression": "this", "frequency": "that"}
      """.stripMargin

    intercept[JsonProcessingException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test("Fails to parse from json with empty expression") {
    val json = "{\"expression\": \"\"}"
    intercept[JsonProcessingException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test("Fails to parse from json with null expression") {
    val json = "{\"expression\": null}"
    intercept[JsonProcessingException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test("Fails to parse from json with missing expression") {
    val json = "{}"
    intercept[JsonProcessingException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test ("renders as json") {
    val expected = "{\"expression\":\"this\",\"frequency\":99000}"
    val json = Json.encode[ExpressionWithFrequency](ExpressionWithFrequency("this", 99000))
    assert(expected === json)
  }

  test ("renders as json with default frequency") {
    val expected = "{\"expression\":\"this\",\"frequency\":60000}"
    val json = Json.encode[ExpressionWithFrequency](ExpressionWithFrequency("this"))
    assert(expected === json)
  }

  test ("renders as json with frequency of 0") {
    val expected = "{\"expression\":\"this\",\"frequency\":60000}"
    val json = Json.encode[ExpressionWithFrequency](ExpressionWithFrequency("this", 0))
    assert(expected === json)
  }

  test ("expression is required to be non-null") {
    intercept[IllegalArgumentException] {
      ExpressionWithFrequency(null)
    }
  }
}
