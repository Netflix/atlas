/*
 * Copyright 2014-2019 Netflix, Inc.
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

import akka.util.ByteString
import org.scalatest.FunSuite

class ByteStringInputStreamSuite extends FunSuite {
  test("read()") {
    val in = new ByteStringInputStream(ByteString("abc"))
    assert('a' === in.read())
    assert('b' === in.read())
    assert('c' === in.read())
    assert(-1 === in.read())
  }

  test("available()") {
    val in = new ByteStringInputStream(ByteString("a"))
    assert(1 === in.available())
    in.read()
    assert(0 === in.available())
  }

  test("read(buf, offset, length)") {
    val in = new ByteStringInputStream(ByteString("abcdefgh"))
    assert(8 == in.available())
    val buf = new Array[Byte](3)

    assert(3 === in.read(buf))
    assert(5 == in.available())
    assert("abc" === new String(buf, 0, 3))

    assert(3 === in.read(buf))
    assert(2 == in.available())
    assert("def" === new String(buf, 0, 3))

    assert(2 === in.read(buf))
    assert(0 == in.available())
    assert("gh" === new String(buf, 0, 2))

    assert(-1 === in.read(buf))
    assert(0 == in.available())
  }
}
