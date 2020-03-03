/*
 * Copyright 2014-2020 Netflix, Inc.
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

import com.fasterxml.jackson.core.JsonToken
import org.scalatest.funsuite.AnyFunSuite

class JsonParserHelperSuite extends AnyFunSuite {

  import JsonParserHelper._

  test("foreachField") {
    val fields = Set.newBuilder[String]
    val parser = Json.newJsonParser("""{"a":0,"b":1}""")
    foreachField(parser) {
      case v =>
        fields += v
        assert(parser.nextIntValue(-1) === v.charAt(0) - 'a')
    }
    assert(fields.result() === Set("a", "b"))
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
        assert(parser.nextIntValue(-1) === 0)
    }
    assert(fields.result() === Set("a"))
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
    assert(builder.result() === List(1, 2, 3))
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

  test("skipNext: empty object") {
    val parser = Json.newJsonParser("""[{}]""")
    assert(parser.nextToken() === JsonToken.START_ARRAY)
    skipNext(parser)
    assert(parser.nextToken() === JsonToken.END_ARRAY)
  }

  test("skipNext: object with simple fields") {
    val parser = Json.newJsonParser("""[{"a":1,"b":"foo","c":false,"d":null}]""")
    assert(parser.nextToken() === JsonToken.START_ARRAY)
    skipNext(parser)
    assert(parser.nextToken() === JsonToken.END_ARRAY)
  }

  test("skipNext: object with complex field") {
    val parser = Json.newJsonParser("""[{"a":{"b":[{"c":{"d":["f",1,null]}}]}}]""")
    assert(parser.nextToken() === JsonToken.START_ARRAY)
    skipNext(parser)
    assert(parser.nextToken() === JsonToken.END_ARRAY)
  }
}
