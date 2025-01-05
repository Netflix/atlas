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

import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

class EventVocabularySuite extends FunSuite {

  private val interpreter = new Interpreter(EventVocabulary.allWords)

  private def parse(str: String): EventExpr.Table = {
    val expr = interpreter.execute(str).stack match {
      case (t: EventExpr.Table) :: Nil => t
      case _                           => throw new IllegalArgumentException(str)
    }
    assertEquals(expr.toString, str)
    expr
  }

  test("table, empty set of columns") {
    val e = intercept[IllegalArgumentException] {
      parse("name,sps,:eq,(,),:table")
    }
    assert(e.getMessage.contains("set of columns cannot be empty"))
  }

  test("table, two columns") {
    val table = parse("name,sps,:eq,(,a,b,),:table")
    assertEquals(table.columns, List("a", "b"))
    assertEquals(table.query, Query.Equal("name", "sps"))
  }

  test("sample, empty set of sampleBy") {
    val e = intercept[IllegalArgumentException] {
      parse("name,sps,:eq,(,),(,message,),:sample")
    }
    assert(e.getMessage.contains("sampleBy cannot be empty"))
  }
}
