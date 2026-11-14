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

class ReadableExprFormatSuite extends FunSuite {

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  private def parse(str: String): Expr = {
    interpreter.execute(str).stack match {
      case (e: Expr) :: Nil => e
      case s                => throw new IllegalArgumentException(s"invalid expr: $s")
    }
  }

  private def check(input: String, expected: String): Unit = {
    val expr = parse(input)
    val readable = ReadableExprFormat(expr)
    assertEquals(readable, expected)
    assertEquals(parse(readable), expr)
  }

  test("toString is unchanged") {
    assertEquals(parse("a,foo,:starts").toString, "a,foo,:re")
  }

  test("literal prefix uses :starts") {
    check("a,foo,:re", "a,foo,:starts")
  }

  test("escaped literal uses :starts") {
    check("a,a\\.b\\.c,:re", "a,a.b.c,:starts")
    check("a,a.b.c,:starts", "a,a.b.c,:starts")
  }

  test("contains") {
    check("a,.*foo,:re", "a,foo,:contains")
    check("a,a.b,:contains", "a,a.b,:contains")
  }

  test("ends") {
    check("a,.*foo$,:re", "a,foo,:ends")
    check("a,a.b,:ends", "a,a.b,:ends")
  }

  test("value with spaces") {
    check("a,foo bar,:starts", "a,foo bar,:starts")
    check("a,foo bar,:contains", "a,foo bar,:contains")
  }

  private val values = List(
    "foo",
    "a.b.c",
    "foo bar",
    " foo ",
    "a,b",
    "a:b",
    ":starts",
    "(",
    ")",
    "a(b)c",
    "^$.|?*+[]{}\\",
    "http:/*",
    "éā"
  )

  values.foreach { v =>
    List("starts", "contains", "ends").foreach { w =>
      test(s"$w: [$v]") {
        val expr = parse(Interpreter.toString("a", v, Interpreter.WordToken(s":$w")))
        val expected = Interpreter.toString("a", v, Interpreter.WordToken(s":$w"))
        check(expected, expected)
        assertEquals(parse(ReadableExprFormat(expr)), expr)
      }
    }
  }

  test("patterns stay as :re") {
    List("a.b", "foo.*bar", "(a|b)", "foo$", ".*", "foo\\d", "foo.*").foreach { p =>
      check(s"a,$p,:re", s"a,$p,:re")
    }
  }

  test(":reic is unchanged") {
    check("a,foo,:reic", "a,foo,:reic")
  }

  test("nested in data expression") {
    check(
      "name,foo,:eq,app,www,:re,:and,status,.*5,:re,:not,:or,:sum,(,app,),:by",
      "name,foo,:eq,app,www,:starts,:and,status,5,:contains,:not,:or,:sum,(,app,),:by"
    )
  }

  test("literal word name as value is not rewritten") {
    val expr = Query.And(Query.Equal("name", ":re"), Query.Equal("app", "foo"))
    assertEquals(ReadableExprFormat(expr), expr.toString)
  }
}
