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

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.ValueInstantiationException
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.json.Json
import com.netflix.atlas.lwcapi.SubscribeApi.*
import munit.FunSuite

class SubscribeApiJsonSuite extends FunSuite {

  test("encode and decode loop") {
    val expressions: List[ExpressionMetadata] = List(
      ExpressionMetadata("this", ExprType.TIME_SERIES, 1234, "idGoesHere"),
      ExpressionMetadata("that", ExprType.TIME_SERIES, 4321, "idGoesHereToo")
    )
    val original = SubscribeRequest("sid", expressions)
    val json = original.toJson
    val decoded = Json.decode[SubscribeRequest](json)
    assertEquals(original, decoded)
  }

  test("decode empty expression list throws") {
    intercept[ValueInstantiationException] {
      Json.decode[SubscribeRequest]("""{"streamId": "sid", "expressions": []}""")
    }
  }

  test("decode missing expression list throws") {
    intercept[ValueInstantiationException] {
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
    assertEquals(decoded.expressions.size, 3)
    assertEquals(decoded.streamId, "testsink")
  }

}
