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
package com.netflix.atlas.json

import java.util.Random
import java.util.UUID

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import munit.FunSuite

class JsonParserHelperSuite extends FunSuite {

  import JsonParserHelper.*

  test("foreachField") {
    val fields = Set.newBuilder[String]
    val parser = Json.newJsonParser("""{"a":0,"b":1}""")
    foreachField(parser) {
      case v =>
        fields += v
        assertEquals(parser.nextIntValue(-1), v.charAt(0) - 'a')
    }
    assertEquals(fields.result(), Set("a", "b"))
  }

  test("foreachField unsupported field") {
    val parser = Json.newJsonParser("""{"b":1}""")
    intercept[IllegalArgumentException] {
      foreachField(parser) {
        case "a" =>
      }
    }
  }

  test("firstField") {
    val fields = Set.newBuilder[String]
    val parser = Json.newJsonParser("""{"a":0,"b":1}""")
    firstField(parser) {
      case "a" =>
        fields += "a"
        assertEquals(parser.nextIntValue(-1), 0)
    }
    assertEquals(fields.result(), Set("a"))
  }

  test("firstField unsupported field") {
    val parser = Json.newJsonParser("""{"b":1}""")
    intercept[IllegalArgumentException] {
      firstField(parser) {
        case "a" =>
      }
    }
  }

  test("foreachItem") {
    val parser = Json.newJsonParser("""[1,2,3]""")
    val builder = List.newBuilder[Int]
    foreachItem(parser) {
      builder += parser.getIntValue
    }
    assertEquals(builder.result(), List(1, 2, 3))
  }

  test("foreachItem, endless loop bug") {
    val parser = Json.newJsonParser("""[1,2,3]""")
    val builder = List.newBuilder[Int]
    intercept[IllegalArgumentException] {
      foreachItem(parser) {
        // if the inner function consumes the end array token it was
        // causing an endless loop
        builder += parser.nextIntValue(-1)
      }
    }
  }

  test("nextDouble: integer") {
    val parser = Json.newJsonParser("""42""")
    assertEquals(nextDouble(parser), 42.0)
  }

  test("nextDouble: big integer") {
    val parser = Json.newJsonParser("""18446744073709552000""")
    assertEquals(nextDouble(parser), 1.8446744073709552e19)
  }

  test("nextDouble: float") {
    val parser = Json.newJsonParser("""42.0""")
    assertEquals(nextDouble(parser), 42.0)
  }

  test("nextDouble: string") {
    val parser = Json.newJsonParser(""""42"""")
    assertEquals(nextDouble(parser), 42.0)
  }

  test("skipNext: empty object") {
    val parser = Json.newJsonParser("""[{}]""")
    assertEquals(parser.nextToken(), JsonToken.START_ARRAY)
    skipNext(parser)
    assertEquals(parser.nextToken(), JsonToken.END_ARRAY)
  }

  test("skipNext: object with simple fields") {
    val parser = Json.newJsonParser("""[{"a":1,"b":"foo","c":false,"d":null}]""")
    assertEquals(parser.nextToken(), JsonToken.START_ARRAY)
    skipNext(parser)
    assertEquals(parser.nextToken(), JsonToken.END_ARRAY)
  }

  test("skipNext: object with complex field") {
    val parser = Json.newJsonParser("""[{"a":{"b":[{"c":{"d":["f",1,null]}}]}}]""")
    assertEquals(parser.nextToken(), JsonToken.START_ARRAY)
    skipNext(parser)
    assertEquals(parser.nextToken(), JsonToken.END_ARRAY)
  }

  test("skipNext: complex object") {
    val parser = Json.newJsonParser("""
        |[
        |  {
        |    "af": {
        |      "query": {
        |        "q1": {
        |          "q1": {
        |            "k": "name",
        |            "v": "discovery.status"
        |          },
        |          "q2": {
        |            "k": "state",
        |            "vs": [
        |              "NOT_REGISTERED",
        |              "DOWN"
        |            ]
        |          }
        |        },
        |        "q2": {
        |          "k": "nf.app",
        |          "v": "sentrytesterspringboot"
        |        }
        |      },
        |      "offset": 0,
        |      "grouped": false
        |    },
        |    "keys": [
        |      "nf.node"
        |    ],
        |    "grouped": true
        |  }
        |]
        |""".stripMargin)
    assertEquals(parser.nextToken(), JsonToken.START_ARRAY)
    skipNext(parser)
    assertEquals(parser.nextToken(), JsonToken.END_ARRAY)
  }

  test("skipNext: random") {
    (0 until 10000).foreach { i =>
      val json = s"""[${randomJson(i)}]"""
      val parser = Json.newJsonParser(json)
      assertEquals(parser.nextToken(), JsonToken.START_ARRAY)
      skipNext(parser)
      assertEquals(parser.nextToken(), JsonToken.END_ARRAY, s"json: $json")
    }
  }

  private def randomJson(seed: Int): JsonNode = {
    val r = new Random(seed) // use fixed seed so issues are reproducible
    randomJson(r)
  }

  private def randomJson(r: Random): JsonNode = {
    r.nextInt(7) match {
      case 0 => randomObject(r)
      case 1 => randomArray(r)
      case 2 => JsonNodeFactory.instance.numberNode(r.nextInt())
      case 3 => JsonNodeFactory.instance.numberNode(r.nextDouble())
      case 4 => JsonNodeFactory.instance.booleanNode(r.nextBoolean())
      case 5 => JsonNodeFactory.instance.nullNode()
      case 6 => JsonNodeFactory.instance.textNode(UUID.randomUUID().toString)
    }
  }

  private def randomObject(r: Random): ObjectNode = {
    val obj = JsonNodeFactory.instance.objectNode()
    (0 until r.nextInt(5)).foreach { i =>
      obj.set[ObjectNode](i.toString, randomJson(r))
    }
    obj
  }

  private def randomArray(r: Random): ArrayNode = {
    val arr = JsonNodeFactory.instance.arrayNode()
    (0 until r.nextInt(5)).foreach { _ =>
      arr.add(randomJson(r))
    }
    arr
  }
}
