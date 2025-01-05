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

import java.time.Duration

import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.json.Json
import munit.FunSuite

class DataSourceSuite extends FunSuite {

  test("parse without explicit step") {
    val id = "123"
    val uri = "http://atlas/api/v1/graph?q=name,sps,:eq"
    val json =
      s"""
        |{
        |  "id": "$id",
        |  "uri": "$uri"
        |}
        |""".stripMargin
    val expected = new DataSource(id, Duration.ofSeconds(60), uri)
    assertEquals(expected, Json.decode[DataSource](json))
  }

  test("parse with explicit step") {
    val id = "123"
    val uri = "http://atlas/api/v1/graph?q=name,sps,:eq"
    val json =
      s"""
         |{
         |  "id": "$id",
         |  "uri": "$uri",
         |  "step": "PT5S"
         |}
         |""".stripMargin
    val expected = new DataSource(id, Duration.ofSeconds(5), uri)
    assertEquals(expected, Json.decode[DataSource](json))
  }

  test("parse with explicit step in uri") {
    val id = "123"
    val uri = "http://atlas/api/v1/graph?q=name,sps,:eq&step=10s"
    val json =
      s"""
         |{
         |  "id": "$id",
         |  "uri": "$uri"
         |}
         |""".stripMargin
    val expected = new DataSource(id, Duration.ofSeconds(10), uri)
    assertEquals(expected, Json.decode[DataSource](json))
  }

  test("parse with explicit step in uri and paylaod") {
    val id = "123"
    val uri = "http://atlas/api/v1/graph?q=name,sps,:eq&step=10s"
    val json =
      s"""
         |{
         |  "id": "$id",
         |  "uri": "$uri",
         |  "step": "PT5S"
         |}
         |""".stripMargin
    // Payload should win
    val expected = new DataSource(id, Duration.ofSeconds(5), uri)
    assertEquals(expected, Json.decode[DataSource](json))
  }
}
