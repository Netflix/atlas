/*
 * Copyright 2014-2017 Netflix, Inc.
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
import org.scalatest.FunSuite

class ExpressionWithFrequencySuite extends FunSuite {
  test("default frequency is applied") {
    val ret1 = ExpressionWithFrequency("this")
    val ret2 = ExpressionWithFrequency("this", ApiSettings.defaultFrequency)
    assert(ret1 === ret2)
  }

  test("default id is applied") {
    val ret1 = ExpressionWithFrequency("this", 5)
    assert(ret1.id.nonEmpty)
  }

  test("full params") {
    val expr = ExpressionWithFrequency("test", 60000, "idHere")
    assert(expr.expression === "test")
    assert(expr.frequency === 60000)
    assert(expr.id === "idHere")
  }

  test("default frequency") {
    val expr = ExpressionWithFrequency("test")
    assert(expr.frequency === ApiSettings.defaultFrequency)
  }

  test("computes id") {
    val expr = ExpressionWithFrequency("test")
    assert(expr.id === "JoTTxcskW9L9buTqMKUA6XrOgUE")
  }

  test("id computation considers frequency") {
    val exp1 = ExpressionWithFrequency("test", 10000)
    val exp2 = ExpressionWithFrequency("test", 20000)
    assert(exp1.id != exp2.id)
  }

  test("parses from json without frequency provides default") {
    val json ="""{"expression": "this"}"""

    val o = Json.decode[ExpressionWithFrequency](json)
    assert(o.expression === "this")
    assert(o.frequency === ApiSettings.defaultFrequency)
  }

  test("parses from json with frequency, without id") {
    val json =
      """
        | {"expression": "this", "frequency": 9999}
      """.stripMargin

    val o = Json.decode[ExpressionWithFrequency](json)

    assert(o.expression === "this")
    assert(o.frequency === 9999)
    assert(o.id.isEmpty)
  }

  test("fails to parse from json with frequency non-integer") {
    val json ="""{"expression": "this", "frequency": "that"}"""
    intercept[InvalidFormatException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test("Fails to parse from json with empty expression") {
    val json = "{\"expression\": \"\"}"
    intercept[IllegalArgumentException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test("Fails to parse from json with null expression") {
    val json = "{\"expression\": null}"
    intercept[IllegalArgumentException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test("Fails to parse from json with missing expression") {
    val json = "{}"
    intercept[IllegalArgumentException] {
      Json.decode[ExpressionWithFrequency](json)
    }
  }

  test("renders as json") {
    val expected = """{"expression":"this","frequency":99000,"id":"foo"}"""
    val json = ExpressionWithFrequency("this", 99000, "foo").toJson
    assert(expected === json)
  }

  test("renders as json with default frequency") {
    val expected = "{\"expression\":\"this\",\"frequency\":60000,\"id\":\"_DoIEIh3HgW9w6qZ_9h3AVff5s4\"}"
    val json = ExpressionWithFrequency("this").toJson
    assert(expected === json)
  }

  test("renders as json with frequency of 0") {
    val expected = "{\"expression\":\"this\",\"frequency\":60000,\"id\":\"_DoIEIh3HgW9w6qZ_9h3AVff5s4\"}"
    val json = ExpressionWithFrequency("this", 0).toJson
    assert(expected === json)
  }

  test("expression is required to be non-null") {
    intercept[IllegalArgumentException] {
      ExpressionWithFrequency(null)
    }
  }

  test("sorts") {
    val source = List(ExpressionWithFrequency("a", 2), ExpressionWithFrequency("z", 1), ExpressionWithFrequency("c", 3))
    val expected = List(ExpressionWithFrequency("a", 2), ExpressionWithFrequency("c", 3), ExpressionWithFrequency("z", 1))
    assert(expected === source.sorted)
  }
}
