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
package com.netflix.atlas.eval.util

import munit.FunSuite

import java.util.UUID

class IdParamSanitizerSuite extends FunSuite {

  test("allowed") {
    assertEquals("foo", IdParamSanitizer.sanitize("foo"))
    assertEquals("foo", IdParamSanitizer.sanitize("Foo"))
    assertEquals("foo2", IdParamSanitizer.sanitize("Foo2"))
    assertEquals("foobarbaz", IdParamSanitizer.sanitize("FooBarBaz"))
    assertEquals("foo_bar-baz", IdParamSanitizer.sanitize("Foo_Bar-Baz"))
    assertEquals("foo.bar.baz", IdParamSanitizer.sanitize("Foo.Bar.Baz"))
  }

  test("uuid") {
    val str = UUID.randomUUID().toString
    assertEquals("default", IdParamSanitizer.sanitize(str))
  }

  test("instance id") {
    val str = String.format("i-%08x", 1234567890)
    assertEquals("default", IdParamSanitizer.sanitize(str))
  }

  test("ipv4") {
    val str = "1.2.3.4"
    assertEquals("default", IdParamSanitizer.sanitize(str))
  }

  test("ipv6") {
    val str = "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
    assertEquals("default", IdParamSanitizer.sanitize(str))
  }

  test("ipv6 local") {
    val str = "::1"
    assertEquals("default", IdParamSanitizer.sanitize(str))
  }

  test("arbitrary number") {
    val str = "foo-12345"
    assertEquals("default", IdParamSanitizer.sanitize(str))
  }
}
