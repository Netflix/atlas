/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.core.model

import org.scalatest.funsuite.AnyFunSuite

class DataExprSuite extends AnyFunSuite {
  test("groupByKey") {
    val expr = DataExpr.Sum(Query.True)
    val tags = Map("name" -> "foo")
    assert(expr.groupByKey(tags) === None)
  }

  test("allKeys") {
    val expr = DataExpr.Sum(Query.Equal("k1", "v1"))
    assert(DataExpr.allKeys(expr) === Set("k1"))
  }

  test("allKeys with GroupBy") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("k1", "v1")), List("k1", "k2"))
    assert(DataExpr.allKeys(expr) === Set("k1", "k2"))
  }
}
