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

class QueryVocabularySuite extends FunSuite {

  val interpreter = new Interpreter(QueryVocabulary.allWords)

  test("contains, escape") {
    var exp = interpreter.execute("a,^$.?*+[](){}\\#&!%,:contains").stack.head
    assertEquals(
      exp.asInstanceOf[Query.Regex].pattern.toString,
      ".*\\^\\$\\.\\?\\*\\+\\[\\]\\(\\)\\{\\}\\\\#&!%"
    )
    exp = interpreter.execute("a,space and ~,:contains").stack.head
    assertEquals(
      exp.asInstanceOf[Query.Regex].pattern.toString,
      ".*space\\u0020and\\u0020~"
    )
  }

  test("contains, matches escaped") {
    val q = interpreter
      .execute("foo,my $var. [work-in-progress],:contains")
      .stack
      .head
      .asInstanceOf[Query.Regex]
    assert(q.matches(Map("foo" -> "my $var. [work-in-progress]")))
    assert(q.matches(Map("foo" -> "initialize my $var. [work-in-progress], not a range")))
    assert(!q.matches(Map("foo" -> "my $var. [work-in progress]")))
  }

  test("starts, prefix and escape") {
    val exp = interpreter.execute("a,[foo],:starts").stack.head
    assertEquals(exp.asInstanceOf[Query.Regex].pattern.prefix(), "[foo]")
    assertEquals(exp.asInstanceOf[Query.Regex].pattern.toString, "^\\[foo\\]")
  }

  test("starts, matches escaped") {
    val q = interpreter
      .execute("foo,my $var.,:starts")
      .stack
      .head
      .asInstanceOf[Query.Regex]
    assert(q.matches(Map("foo" -> "my $var.")))
    assert(!q.matches(Map("foo" -> "initialize my $var. [work-in-progress], not a range")))
    assert(q.matches(Map("foo" -> "my $var. [work-in progress]")))
  }

  test("ends, suffix and escape") {
    val exp = interpreter.execute("a,[foo],:ends").stack.head
    assertEquals(exp.asInstanceOf[Query.Regex].pattern.prefix(), null)
    assertEquals(exp.asInstanceOf[Query.Regex].pattern.toString, ".*\\[foo\\]$")
  }

  test("ends, matches escaped") {
    val q = interpreter
      .execute("foo,my $var.,:ends")
      .stack
      .head
      .asInstanceOf[Query.Regex]
    assert(q.matches(Map("foo" -> "my $var.")))
    assert(!q.matches(Map("foo" -> "initialize my $var. [work-in-progress], not a range")))
    assert(!q.matches(Map("foo" -> "my $var. [work-in progress]")))
  }

  private def execEquals(str: String): Query.Equal = {
    val exp1 = interpreter.execute(str).stack.head
    val exp2 = interpreter.execute(exp1.toString).stack.head
    assertEquals(exp1, exp2)
    exp1.asInstanceOf[Query.Equal]
  }

  test("eq, with escaped comma") {
    val txt = Interpreter.escape("foo, bar, baz")
    val exp = execEquals(s"a,$txt,:eq")
    assertEquals(exp.v, "foo, bar, baz")
  }

  test("contains, with escaped comma") {
    val txt = Interpreter.escape("foo, bar, baz")
    val exp = interpreter.execute(s"a,$txt,:contains").stack.head
    assertEquals(
      exp.asInstanceOf[Query.Regex].pattern.toString,
      ".*foo,\\u0020bar,\\u0020baz"
    )
  }

  test("eq, with escaped colon in value") {
    val txt = Interpreter.escape(":foo")
    val exp = execEquals(s"a,$txt,:eq")
    assertEquals(exp.v, ":foo")
  }

  test("eq, with escaped colon in key") {
    val txt = Interpreter.escape(":foo")
    val exp = execEquals(s"$txt,a,:eq")
    assertEquals(exp.k, ":foo")
  }

  test("eq, with escaped open paren in value") {
    val txt = Interpreter.escape("(")
    val exp = execEquals(s"a,$txt,:eq")
    assertEquals(exp.v, "(")
  }

  test("eq, with escaped close paren in value") {
    val txt = Interpreter.escape(")")
    val exp = execEquals(s"a,$txt,:eq")
    assertEquals(exp.v, ")")
  }
}
