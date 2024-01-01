/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.eval.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

class SortedTagMapDeserializerSuite extends FunSuite {

  private val mapper = {
    val module = new SimpleModule()
      .addDeserializer(classOf[SortedTagMap], new SortedTagMapDeserializer(2))
    new ObjectMapper().registerModule(module)
  }

  private def parse(json: String): SortedTagMap = {
    mapper.readValue(json, classOf[SortedTagMap])
  }

  test("empty") {
    val json =
      """
        |{}
        |""".stripMargin
    val actual = parse(json)
    assertEquals(actual, SortedTagMap.empty)
  }

  test("one") {
    val json =
      """
        |{
        |  "a": "1"
        |}
        |""".stripMargin
    val actual = parse(json)
    assertEquals(actual, SortedTagMap("a" -> "1"))
  }

  test("many") {
    val json =
      """
        |{
        |  "d": "4",
        |  "c": "3",
        |  "b": "2",
        |  "a": "1"
        |}
        |""".stripMargin
    val actual = parse(json)
    assertEquals(actual, SortedTagMap("a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4"))
  }
}
