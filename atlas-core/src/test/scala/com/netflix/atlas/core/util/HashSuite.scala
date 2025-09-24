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

class HashSuite extends FunSuite {

  test("md5") {
    assert(Hash.md5("42").toString(10) == "215089739385482443301854222253501995174")
  }

  test("sha1") {
    assert(Hash.sha1("42").toString(10) == "838146913046966959093018715372872234545534447190")
  }

  test("sha1bytes zeroPad") {
    val expected = Strings.zeroPad(Hash.sha1("42"), 40)
    val actual = Strings.zeroPad(Hash.sha1bytes("42"), 40)
    assertEquals(actual, expected)
  }
}
