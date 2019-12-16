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

import com.fasterxml.jackson.databind.JsonMappingException
import com.netflix.atlas.json.Json
import com.netflix.atlas.lwcapi.SubscribeApi._
import org.scalatest.funsuite.AnyFunSuite

class SubscribeApiJsonSuite extends AnyFunSuite {
  test("encode and decode loop") {
    val expressions: List[ExpressionMetadata] = List(
      ExpressionMetadata("this", 1234, "idGoesHere"),
      ExpressionMetadata("that", 4321, "idGoesHereToo")
    )
    val original = SubscribeRequest("sid", expressions)
    val json = original.toJson
    val decoded = Json.decode[SubscribeRequest](json)
    assert(original === decoded)
  }

  test("decode empty expression list throws") {
    intercept[IllegalArgumentException] {
      Json.decode[SubscribeRequest]("""{"streamId": "sid", "expressions": []}""")
    }
  }

  test("decode missing expression list throws") {
    intercept[IllegalArgumentException] {
      Json.decode[SubscribeRequest]("{}")
    }
  }

  test("decode array") {
    intercept[JsonMappingException] {
      Json.decode[SubscribeRequest]("[]")
    }
  }

  test("decode with streamId") {
    val decoded = Json.decode[SubscribeRequest]("""
      {
        "streamId": "testsink",
        "expressions": [
          { "expression": "this", "frequency": 12345 },
          { "expression": "that", "frequency": 54321 },
          { "expression": "those" }
        ]
      }
      """)
    assert(decoded.expressions.size === 3)
    assert(decoded.streamId === "testsink")
  }

}
