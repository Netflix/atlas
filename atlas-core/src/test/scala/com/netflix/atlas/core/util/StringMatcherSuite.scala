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
package com.netflix.atlas.core.util

import java.util.regex.Pattern

import org.scalatest.FunSuite


class StringMatcherSuite extends FunSuite {

  import com.netflix.atlas.core.util.StringMatcher._

  private def re(s: String): Pattern = Pattern.compile(s)
  private def reic(s: String): Pattern = Pattern.compile(s, Pattern.CASE_INSENSITIVE)

  test("matches All") {
    assert(All.matches("foo"))
  }

  test("matches StartsWith") {
    assert(StartsWith("f").matches("foo"))
    assert(StartsWith("foo").matches("foo"))
    assert(!StartsWith("f").matches("bar"))
    assert(!StartsWith("bar").matches("foobar"))
  }

  test("matches IndexOf") {
    assert(IndexOf("f").matches("foo"))
    assert(IndexOf("foo").matches("foo"))
    assert(IndexOf("oo").matches("foo"))
    assert(!IndexOf("f").matches("bar"))
    assert(!IndexOf("bar").matches("fooBar"))
  }

  test("matches IndexOfIgnoreCase") {
    assert(IndexOfIgnoreCase("f").matches("foo"))
    assert(IndexOfIgnoreCase("foo").matches("foo"))
    assert(IndexOfIgnoreCase("oO").matches("foo"))
    assert(!IndexOfIgnoreCase("F").matches("bar"))
    assert(IndexOfIgnoreCase("bar").matches("fooBar"))
  }

  test("matches Regex") {
    assert(Regex(None, re("^f")).matches("foo"))
    assert(Regex(None, re("f")).matches("foo"))
    assert(Regex(None, re("foo")).matches("foo"))
    assert(Regex(None, re("oo")).matches("foo"))
    assert(!Regex(None, re("Foo")).matches("foo"))
  }

  test("matches RegexIgnoreCase") {
    assert(Regex(None, reic("^f")).matches("foo"))
    assert(Regex(None, reic("f")).matches("foo"))
    assert(Regex(None, reic("foo")).matches("foo"))
    assert(Regex(None, reic("oo")).matches("foO"))
    assert(Regex(None, reic("Foo")).matches("foo"))
  }

  test("compile All") {
    assert(compile(".*") === All)
    assert(compile("^.*$") === All)
    assert(compile("^^^.*$$$") === All)
  }

  test("compile StartsWith") {
    assert(compile("^foo.*") === StartsWith("foo"))
  }

  test("compile StartsWithIgnoreCase") {
    assert(compile("^foo.*", false) === Regex(None, reic("^foo.*")))
  }

  test("compile IndexOf") {
    assert(compile("foo") === IndexOf("foo"))
    assert(compile(".*foo.*") === IndexOf("foo"))
    assert(compile("^.*foo.*$") === IndexOf("foo"))
  }

  test("compile IndexOfIgnoreCase") {
    assert(compile("foo", false) === IndexOfIgnoreCase("foo"))
    assert(compile(".*foo.*", false) === IndexOfIgnoreCase("foo"))
    assert(compile("^.*foo.*$", false) === IndexOfIgnoreCase("foo"))
  }

  test("compile Prefix") {
    val prefix = Some("foo")
    assert(compile("^foo[bar]") === Regex(prefix, re("^foo[bar]")))
    assert(compile("^foo[bar].*") === Regex(prefix, re("^foo[bar].*")))
    assert(compile("^foo[bar].*$") === Regex(prefix, re("^foo[bar].*$")))
  }

  test("compile PrefixIgnoreCase") {
    assert(compile("^foo[bar]", false) === Regex(None, reic("^foo[bar]")))
    assert(compile("^foo[bar].*", false) === Regex(None, reic("^foo[bar].*")))
    assert(compile("^foo[bar].*$", false) === Regex(None, reic("^foo[bar].*$")))
  }

  test("compile Regex") {
    assert(compile("^.*foo[bar]") === Regex(None, re("^.*foo[bar]")))
    assert(compile("^.*foo[bar].*") === Regex(None, re("^.*foo[bar].*")))
    assert(compile("^.*foo[bar].*$") === Regex(None, re("^.*foo[bar].*$")))
  }

  test("compile RegexIgnoreCase") {
    assert(compile("^.*foo[bar]", false) === Regex(None, reic("^.*foo[bar]")))
    assert(compile("^.*foo[bar].*", false) === Regex(None, reic("^.*foo[bar].*")))
    assert(compile("^.*foo[bar].*$", false) === Regex(None, reic("^.*foo[bar].*$")))
  }

}
