/*
 * Copyright 2014-2019 Netflix, Inc.
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

class FilterSuite extends AnyFunSuite {

  private val start = 0L
  private val step = 60000L
  private val context = EvalContext(start, start + step, step)

  test("empty result from filtering constant expression") {
    val expr = FilterExpr.Filter(
      MathExpr.Constant(1),
      MathExpr.GreaterThan(MathExpr.Constant(1), MathExpr.Constant(2))
    )
    assert(expr.eval(context, Nil).data.isEmpty)
  }

  test("empty filtered result with binary operation, lhs") {
    val filteredExpr = FilterExpr.Filter(
      MathExpr.Constant(1),
      MathExpr.GreaterThan(MathExpr.Constant(1), MathExpr.Constant(2))
    )
    val expr = MathExpr.Add(filteredExpr, MathExpr.Constant(2))
    assert(expr.eval(context, Nil).data.isEmpty)
  }

  test("empty filtered result with binary operation, rhs") {
    val filteredExpr = FilterExpr.Filter(
      MathExpr.Constant(1),
      MathExpr.GreaterThan(MathExpr.Constant(1), MathExpr.Constant(2))
    )
    val expr = MathExpr.Add(MathExpr.Constant(2), filteredExpr)
    assert(expr.eval(context, Nil).data.isEmpty)
  }

  test("empty filtered result with binary operation, both sides") {
    val filteredExpr = FilterExpr.Filter(
      MathExpr.Constant(1),
      MathExpr.GreaterThan(MathExpr.Constant(1), MathExpr.Constant(2))
    )
    val expr = MathExpr.Add(filteredExpr, filteredExpr)
    assert(expr.eval(context, Nil).data.isEmpty)
  }
}
