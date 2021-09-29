/*
 * Copyright 2014-2021 Netflix, Inc.
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

import org.scalatest.funsuite.AnyFunSuite

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.util.Using

class ByteBufferInputStreamSuite extends AnyFunSuite {

  private def wrap(str: String): ByteBuffer = {
    ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8))
  }

  test("read()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assert(in.read() === 'a')
    assert(in.read() === 'b')
    assert(in.read() === 'c')
    assert(in.read() === -1)
  }

  test("read(buf)") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    val array = new Array[Byte](2)
    assert(in.read(array) === 2)
    assert(array === Array('a', 'b'))
    assert(in.read(array) === 1)
    assert(array === Array('c', 'b')) // b left over from previous
    assert(in.read(array) === -1)
  }

  test("read(buf, offset, length)") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    val array = new Array[Byte](5)
    assert(in.read(array, 2, 3) === 3)
    assert(array === Array('\u0000', '\u0000', 'a', 'b', 'c'))
    assert(in.read(array) === -1)
  }

  test("available()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assert(in.available() === 3)
    assert(in.skip(2) === 2)
    assert(in.available() === 1)
    assert(in.read() === 'c')
    assert(in.available() === 0)
  }

  test("skip()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assert(in.skip(2) === 2)
    assert(in.read() === 'c')
    assert(in.read() === -1)
    assert(in.skip(2) === 0)
  }

  test("mark() and reset()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assert(in.markSupported())
    assert(in.read() === 'a')

    in.mark(5)
    assert(in.read() === 'b')
    assert(in.read() === 'c')

    in.reset()
    assert(in.read() === 'b')

    in.reset()
    assert(in.read() === 'b')
  }

  test("close()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    (0 until 10).foreach { _ =>
      Using.resource(in) { r =>
        assert(r.read() === 'a')
        r.skip(100)
        assert(r.read() === -1)
      }
    }
  }
}
