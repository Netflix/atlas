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
package com.netflix.atlas.core.util

import munit.FunSuite

class InternerSuite extends FunSuite {

  test("noop") {
    val i = new NoopInterner[String]
    val s1 = new String("foo")
    val s2 = new String("foo")
    assert(s1 ne s2)
    assert(i.intern(s1) ne i.intern(s2))
  }

  test("string") {
    val i = StringInterner
    val s1 = new String("foo")
    val s2 = new String("foo")
    assert(s1 ne s2)
    assert(i.intern(s1) eq i.intern(s2))
  }

  test("predef hit") {
    val s1 = new String("foo")
    val s2 = new String("foo")
    val i = new PredefinedInterner[String](List(s1), new NoopInterner[String])
    assert(s1 ne s2)
    assert(i.intern(s1) eq s1)
    assert(i.intern(s2) eq s1)
  }

  test("predef miss") {
    val s1 = new String("bar")
    val s2 = new String("bar")
    val i = new PredefinedInterner[String](List("foo"), new NoopInterner[String])
    assert(s1 ne s2)
    assert(i.intern(s1) eq s1)
    assert(i.intern(s2) eq s2)
  }

  test("caffeine") {
    val s1 = new String("foo")
    val s2 = new String("foo")
    val i = new CaffeineInterner[String](10)
    assert(s1 ne s2)
    assert(i.intern(s1) eq s1)
    assert(i.intern(s2) eq s1)
  }
}
