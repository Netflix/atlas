/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

class DataExprSuite extends FunSuite {

  test("groupByKey") {
    val expr = DataExpr.Sum(Query.True)
    val tags = Map("name" -> "foo")
    assertEquals(expr.groupByKey(tags), None)
  }

  test("GroupBy exprString round-trips keys that need escaping") {
    val interpreter = Interpreter(StyleVocabulary.allWords)
    def dataExpr(s: String): DataExpr = interpreter.execute(s).stack match {
      case ModelDataTypes.PresentationType(t) :: Nil => t.expr.asInstanceOf[DataExpr]
      case _                                         => throw new IllegalArgumentException(s)
    }

    // The single tag key contains a comma (escaped on input). Parsing unescapes it,
    // so the rendered expression must re-escape it; otherwise re-parsing splits it
    // into two keys.
    val expr = dataExpr("name,sps,:eq,:sum,(,a\\u002cb,),:by")
    assertEquals(expr.asInstanceOf[DataExpr.GroupBy].keys, List("a,b"))
    assertEquals(dataExpr(expr.exprString), expr)
  }

  test("Equal exprString round-trips a value containing a literal unicode escape (#1926)") {
    val interpreter = Interpreter(StyleVocabulary.allWords)
    def dataExpr(s: String): DataExpr = interpreter.execute(s).stack match {
      case ModelDataTypes.PresentationType(t) :: Nil => t.expr.asInstanceOf[DataExpr]
      case _                                         => throw new IllegalArgumentException(s)
    }

    // The query value literally contains backslash-u-0041 (six chars), NOT the character 'A'.
    // escape now escapes the leading backslash, so the rendered exprString re-parses (via
    // unescape) back to the same value instead of decoding to "A".
    val eq = DataExpr.Sum(Query.Equal("name", "\\" + "u0041"))
    assertEquals(dataExpr(eq.exprString), eq)

    // `:in` renders its values through a different escape path (Interpreter.append over a
    // list); a literal unicode escape in a list value must round-trip there too.
    val in = DataExpr.Sum(Query.In("name", List("\\" + "u0041", "bar")))
    assertEquals(dataExpr(in.exprString), in)
  }

  test("allKeys") {
    val expr = DataExpr.Sum(Query.Equal("k1", "v1"))
    assertEquals(DataExpr.allKeys(expr), Set("k1"))
  }

  test("allKeys with GroupBy") {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("k1", "v1")), List("k1", "k2"))
    assertEquals(DataExpr.allKeys(expr), Set("k1", "k2"))
  }
}
