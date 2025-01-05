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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import munit.FunSuite

class ExprRewriteSuite extends FunSuite {

  test("basic") {
    val avgHashCode = System.identityHashCode(ConsolidationFunction.Avg)
    val expr = DataExpr.Sum(Query.True)
    val result = expr.rewrite {
      case _: Query => Query.False
    }
    assertEquals(result, DataExpr.Sum(Query.False))
    assertEquals(System.identityHashCode(result.asInstanceOf[AggregateFunction].cf), avgHashCode)
    ConsolidationFunction.Avg
  }

  // The expr rewrite uses reflection and can result in duplicate case objects leading to
  // confusing MatchErrors or other subtle failures later on.
  test("rewrite should not cause duplicates case objects") {
    val trueHashCode = System.identityHashCode(Query.True)
    val expr = Query.And(Query.True, Query.False)
    val result = expr.rewrite {
      case Query.Not(_) => Query.False
    }
    assertEquals(result, Query.And(Query.True, Query.False))
    assertEquals(System.identityHashCode(result.asInstanceOf[Query.And].q1), trueHashCode)
  }

}
