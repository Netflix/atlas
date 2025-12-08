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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.pekko.util.ByteString
import com.netflix.atlas.json.Json
import munit.FunSuite

import java.nio.charset.StandardCharsets

class ReplayLoggingSuite extends FunSuite {

  test("isSmile: detects SMILE format with signature") {
    val smileData = ByteString(Json.smileEncode(Map("key" -> "value")))
    assert(ReplayLogging.isSmile(smileData))
  }

  test("isSmile: returns false for plain JSON") {
    val jsonData = ByteString("""{"key":"value"}""", StandardCharsets.UTF_8)
    assert(!ReplayLogging.isSmile(jsonData))
  }

  test("isSmile: returns false for empty ByteString") {
    assert(!ReplayLogging.isSmile(ByteString.empty))
  }

  test("isSmile: returns false for single byte") {
    assert(!ReplayLogging.isSmile(ByteString(':')))
  }

  test("isSmile: returns false for ByteString starting with ':' but not ')'") {
    val data = ByteString(":x", StandardCharsets.UTF_8)
    assert(!ReplayLogging.isSmile(data))
  }

  test("asString: converts SMILE ByteString to JSON string") {
    val original = Map("name" -> "test", "value" -> 42)
    val smileData = ByteString(Json.smileEncode(original))
    val result = ReplayLogging.asString(smileData)

    // Parse the result back to verify it's valid JSON
    val decoded = Json.decode[Map[String, Any]](result)
    assertEquals(decoded("name"), "test")
    assertEquals(decoded("value"), 42)
  }

  test("asString: converts nested SMILE ByteString to JSON string") {
    val original = Map(
      "outer" -> Map("inner" -> "value"),
      "array" -> List(1, 2, 3)
    )
    val smileData = ByteString(Json.smileEncode(original))
    val result = ReplayLogging.asString(smileData)

    // Verify it's valid JSON by decoding
    val decoded = Json.decode[JsonNode](result)
    assert(decoded.has("outer"))
    assert(decoded.has("array"))
  }

  test("asString: returns plain text for non-SMILE ByteString") {
    val plainText = "plain text message"
    val data = ByteString(plainText, StandardCharsets.UTF_8)
    val result = ReplayLogging.asString(data)
    assertEquals(result, plainText)
  }

  test("asString: returns JSON string for non-SMILE JSON ByteString") {
    val json = """{"key":"value"}"""
    val data = ByteString(json, StandardCharsets.UTF_8)
    val result = ReplayLogging.asString(data)
    assertEquals(result, json)
  }

  test("asString: handles empty ByteString") {
    val result = ReplayLogging.asString(ByteString.empty)
    assertEquals(result, "")
  }

  test("log(ByteString): returns input unchanged") {
    val data = ByteString("test", StandardCharsets.UTF_8)
    val result = ReplayLogging.log(data)
    assertEquals(result, data)
  }

  test("log(ByteString): handles SMILE input") {
    val smileData = ByteString(Json.smileEncode(Map("key" -> "value")))
    val result = ReplayLogging.log(smileData)
    assertEquals(result, smileData)
  }

  test("log(String): trims and returns input") {
    val input = "  test message  "
    val result = ReplayLogging.log(input)
    assertEquals(result, "test message")
  }

  test("log(String): handles empty string") {
    val result = ReplayLogging.log("")
    assertEquals(result, "")
  }

  test("log(String): handles whitespace-only string") {
    val result = ReplayLogging.log("   ")
    assertEquals(result, "")
  }
}
