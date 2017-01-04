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
package com.netflix.atlas.json

import org.scalatest.FunSuite


class JsonParserHelperSuite extends FunSuite {

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
}
