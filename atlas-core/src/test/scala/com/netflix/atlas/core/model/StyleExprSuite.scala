/*
 * Copyright 2015 Netflix, Inc.
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

import java.time.Duration

import org.scalatest.FunSuite

class StyleExprSuite extends FunSuite {

  private val oneDay = Duration.ofDays(1)
  private val oneWeek = Duration.ofDays(7)

  test("perOffset") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map("offset" -> "(,0h,1d,1w,)"))
    val expected = List(
      StyleExpr(DataExpr.Sum(Query.True), Map.empty),
      StyleExpr(DataExpr.Sum(Query.True).withOffset(oneDay), Map.empty),
      StyleExpr(DataExpr.Sum(Query.True).withOffset(oneWeek), Map.empty)
    )
    assert(expr.perOffset === expected)
  }

  test("perOffset empty") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map("offset" -> "(,)"))
    val expected = List(expr)
    assert(expr.perOffset === expected)
  }

  test("perOffset not specified") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map.empty)
    val expected = List(expr)
    assert(expr.perOffset === expected)
  }
}

