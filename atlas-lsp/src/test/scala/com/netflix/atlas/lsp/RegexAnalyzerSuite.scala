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
package com.netflix.atlas.lsp

import munit.FunSuite

class RegexAnalyzerSuite extends FunSuite {

  private def replacement(pattern: String): Option[(String, String)] = {
    RegexAnalyzer.analyze(pattern).map(s => s.value -> s.word)
  }

  test("literal prefix") {
    assertEquals(replacement("foo"), Some("foo" -> "starts"))
  }

  test("unescaped dots") {
    assertEquals(replacement("a.b.c"), Some("a.b.c" -> "starts"))
    val s = RegexAnalyzer.analyze("a.b.c").get
    assert(s.message.contains("any character"), s.message)
  }

  test("escaped dots") {
    assertEquals(replacement("a\\.b\\.c"), Some("a.b.c" -> "starts"))
  }

  test("trailing wildcard") {
    assertEquals(replacement("a.b.*"), Some("a.b" -> "starts"))
    assertEquals(replacement("a.b.*$"), Some("a.b" -> "starts"))
  }

  test("leading caret") {
    assertEquals(replacement("^foo"), Some("foo" -> "starts"))
  }

  test("contains") {
    assertEquals(replacement(".*foo"), Some("foo" -> "contains"))
  }

  test("ends") {
    assertEquals(replacement(".*foo$"), Some("foo" -> "ends"))
  }

  test("exact") {
    assertEquals(replacement("foo$"), Some("foo" -> "eq"))
  }

  test("escaped dollar is literal") {
    assertEquals(replacement("foo\\$"), Some("foo$" -> "starts"))
  }

  test("complex patterns are ignored") {
    List("foo|bar", "(a|b)", "[ab]c", "a.+", "foo.*bar", "\\d+", "a{2}", "4(?!04)", ".*", "")
      .foreach { p =>
        assertEquals(replacement(p), None, p)
      }
  }

  test("glob star at end") {
    val s = RegexAnalyzer.analyze("foo*").get
    assert(s.message.contains("'o*'"), s.message)
    assertEquals(s.value -> s.word, "foo" -> "starts")
  }

  test("glob star in middle") {
    assertEquals(replacement("foo*bar"), Some("foo.*bar" -> "re"))
  }

  test("glob star with dots") {
    val s = RegexAnalyzer.analyze("a.b*").get
    assertEquals(s.value -> s.word, "a.b" -> "starts")
    assert(s.message.contains("'.' literally"), s.message)
  }

  test("glob star without dots does not mention dots") {
    val s = RegexAnalyzer.analyze("foo*").get
    assert(!s.message.contains("'.' literally"), s.message)
  }

  test("glob star with leading caret") {
    assertEquals(replacement("^foo*"), Some("foo" -> "starts"))
    assertEquals(replacement("^*"), None)
  }

  test("glob star after supplementary character") {
    val emoji = new String(Character.toChars(0x1F600))
    val s = RegexAnalyzer.analyze(s"a$emoji*").get
    assert(s.message.startsWith(s"'$emoji*'"), s.message)
    assertEquals(s.value -> s.word, s"a$emoji" -> "starts")
  }

  test("glob star after escaped char") {
    val s = RegexAnalyzer.analyze("foo\\.*").get
    assert(s.message.contains("'\\.*'"), s.message)
    assertEquals(s.value -> s.word, "foo." -> "starts")
  }

  test("glob star ignored for complex patterns") {
    assertEquals(replacement("(foo|bar)*"), None)
    assertEquals(replacement("[ab]*"), None)
  }

  test("multiple glob stars") {
    val s = RegexAnalyzer.analyze("foo*bar*").get
    assert(s.message.contains("'o*', 'r*'"), s.message)
    assertEquals(s.value -> s.word, "foo.*bar" -> "re")
  }

  test("multiple glob stars with leading wildcard") {
    assertEquals(replacement("*foo*"), None)
    assertEquals(replacement(".*foo*"), Some("foo" -> "contains"))
  }

  test("equivalent message for exact transforms") {
    val s = RegexAnalyzer.analyze("foo.*").get
    assertEquals(s.value -> s.word, "foo" -> "starts")
    assert(s.message.contains("equivalent"), s.message)
  }

  test("valid escapes use pattern matcher") {
    assertEquals(replacement("foo\\-bar"), Some("foo-bar" -> "starts"))
    assertEquals(replacement("foo\\/bar"), Some("foo/bar" -> "starts"))
  }

  test("wildcard dots are not treated as literal") {
    List("5..", "foo.", "a..b", "foo.$", ".foo").foreach { p =>
      assertEquals(replacement(p), None, p)
    }
  }

  test("dots between word characters") {
    assertEquals(replacement("v1.2.3"), Some("v1.2.3" -> "starts"))
    assertEquals(replacement(".*a.b$"), Some("a.b" -> "ends"))
    assertEquals(replacement("a.b$"), Some("a.b" -> "eq"))
  }

  test("glob message mentions optional") {
    val s = RegexAnalyzer.analyze("https*").get
    assert(s.message.contains("'?'"), s.message)
  }

  test("glob star with dollar in middle is ignored") {
    assertEquals(replacement("foo$bar*"), None)
  }

  test("glob star with trailing dollar") {
    assertEquals(replacement("foo*$"), Some("foo" -> "starts"))
  }
}
