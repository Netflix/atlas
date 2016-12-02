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

import com.fasterxml.jackson.databind.JsonMappingException
import com.netflix.atlas.lwcapi.SubscribeApi._
import org.scalatest.FunSuite

class SubscribeApiJsonSuite extends FunSuite {
  test("encode and decode loop") {
    val expressions: List[ExpressionWithFrequency] = List(
      ExpressionWithFrequency("this", 1234, "idGoesHere"),
      ExpressionWithFrequency("that", 4321, "idGoesHereToo")
    )
    val original = SubscribeRequest("sid", expressions)
    val json = original.toJson
    val decoded = SubscribeRequest.fromJson(json)
    assert(original === decoded)
  }

  test("decode empty expression list throws") {
    intercept[IllegalArgumentException] {
      SubscribeApi.SubscribeRequest.fromJson("""{"streamId": "sid", "expressions": []}""")
    }
  }

  test("decode missing expression list throws") {
    intercept[IllegalArgumentException] {
      SubscribeApi.SubscribeRequest.fromJson("{}")
    }
  }

  test("decode array") {
    intercept[JsonMappingException] {
      SubscribeApi.SubscribeRequest.fromJson("[]")
    }
  }

  test("decode with streamId") {
    val decoded = SubscribeApi.SubscribeRequest.fromJson("""
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
