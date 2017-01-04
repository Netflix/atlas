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
package com.netflix.atlas.webapi

import com.netflix.atlas.json.Json
import org.scalatest.FunSuite

class GraphResponseSuite extends FunSuite {
  test("parse") {
    val res = Json.decode[GraphApi.Response](
      """
        |{
        |  "start": 1428791640000,
        |  "step": 60000,
        |  "legend": ["one"],
        |  "metrics": [{"name": "one"}],
        |  "values": [
        |    [0.0],
        |    [1.0],
        |    [2.0]
        |  ]
        |}
      """.stripMargin)
    assert(res.legend === List("one"))
    assert(res.metrics === List(Map("name" -> "one")))
  }
}
