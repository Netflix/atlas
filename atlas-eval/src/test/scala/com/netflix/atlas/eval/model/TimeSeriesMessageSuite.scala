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
package com.netflix.atlas.eval.model

import munit.FunSuite

class TimeSeriesMessageSuite extends FunSuite {

  private val baseMessage = TimeSeriesMessage(
    id = "12345",
    query = "name,sps,:eq,(,cluster,),:by",
    groupByKeys = List("cluster"),
    start = 0L,
    end = 60000L,
    step = 60000L,
    label = "test",
    tags = Map("name" -> "sps", "cluster" -> "www"),
    data = ArrayData(Array(42.0)),
    None,
    Nil
  )

  test("json encoding with empty group by") {
    val input = baseMessage.copy(groupByKeys = Nil)
    assert(!input.toJson.contains("groupByKeys"))
  }

  test("json encoding with group by") {
    val input = baseMessage
    assert(input.toJson.contains(""""groupByKeys":["cluster"]"""))
  }
}
