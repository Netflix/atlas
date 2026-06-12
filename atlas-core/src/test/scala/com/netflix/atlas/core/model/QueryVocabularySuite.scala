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

  test("eq and in handle escapes consistently") {
    // `:` is the escape sequence for ':'. With :eq the value is unescaped
    // to ":foo". The same value passed through a list to :in should behave the
    // same way.
    val eq = interpreter.execute("name,\\u003afoo,:eq").stack.head.asInstanceOf[Query.Equal]
    assertEquals(eq.v, ":foo")

    // :in with a single value optimizes to Query.Equal and should match :eq.
    val in = interpreter.execute("name,(,\\u003afoo,),:in").stack.head.asInstanceOf[Query.Equal]
    assertEquals(in.v, ":foo")
  }

  test("in unescapes all values, not just the single-value opt case") {
    // With multiple values :in produces a Query.In and each value in the set
    // should be unescaped the same way a scalar token would be.
    val in = interpreter
      .execute("name,(,\\u003afoo,\\u003abar,),:in")
      .stack
      .head
      .asInstanceOf[Query.In]
    assertEquals(in.vs, List(":foo", ":bar"))
  }

  test("in unescapes each value exactly once, matching eq") {
    // Strings.unescape is not idempotent: the token used below decodes in one pass
    // to a 6-character string that still contains a valid escape sequence, which a
    // second pass would decode further. Asserting the once-decoded form pins the
    // value to exactly one unescape, matching the scalar :eq path. The earlier tests
    // use a colon escape whose decoded form is a fixed point, so they cannot detect a
    // double unescape.
    val eq = interpreter.execute("name,\\u005cu0041,:eq").stack.head.asInstanceOf[Query.Equal]
    assertEquals(eq.v, "\\u0041")

    val in = interpreter
      .execute("name,(,\\u005cu0041,\\u005cu0042,),:in")
      .stack
      .head
      .asInstanceOf[Query.In]
    assertEquals(in.vs, List("\\u0041", "\\u0042"))
  }

  test("in with empty list is Query.False") {
    assertEquals(interpreter.execute("name,(,),:in").stack.head, Query.False)
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

  test("starts with space") {
    val re = interpreter.execute("a,a b,:re").stack.head.asInstanceOf[Query.Regex]
    val exp = interpreter.execute("a,a b,:starts").stack.head.asInstanceOf[Query.Regex]
    assertEquals(exp.pattern.prefix(), re.pattern.prefix())
    assertEquals(exp.pattern.toString, re.pattern.toString)
    assertEquals(exp.toString, re.toString)
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

  test("ends with space") {
    val re = interpreter.execute("a,.*a b$,:re").stack.head.asInstanceOf[Query.Regex]
    val exp = interpreter.execute("a,a b,:ends").stack.head.asInstanceOf[Query.Regex]
    assertEquals(exp.pattern.prefix(), re.pattern.prefix())
    assertEquals(exp.pattern.toString, re.pattern.toString)
    assertEquals(exp.toString, re.toString)
  }

  private def execEquals(str: String): Query.Equal = {
    val exp1 = interpreter.execute(str).stack.head
    val exp2 = interpreter.execute(exp1.toString).stack.head
    assertEquals(exp1, exp2)
    exp1.asInstanceOf[Query.Equal]
  }

  test("eq, with escaped whitespace") {
    val txt = Interpreter.escape("\n \t foo bar \t \n")
    val exp = execEquals(s"a,$txt,:eq")
    assertEquals(exp.v, "\n \t foo bar \t \n")
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

  test("contains with space") {
    val re = interpreter.execute("a,.*a b,:re").stack.head.asInstanceOf[Query.Regex]
    val exp = interpreter.execute("a,a b,:contains").stack.head.asInstanceOf[Query.Regex]
    assertEquals(exp.pattern.prefix(), re.pattern.prefix())
    assertEquals(exp.pattern.toString, re.pattern.toString)
    assertEquals(exp.toString, re.toString)
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
