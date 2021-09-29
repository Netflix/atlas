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

import java.nio.CharBuffer
import scala.util.Using

class CharBufferReaderSuite extends AnyFunSuite {

  test("read()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assert(reader.read() === 'a')
    assert(reader.read() === 'b')
    assert(reader.read() === 'c')
    assert(reader.read() === -1)
  }

  test("read(cbuf)") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    val array = new Array[Char](2)
    assert(reader.read(array) === 2)
    assert(array === Array('a', 'b'))
    assert(reader.read(array) === 1)
    assert(array === Array('c', 'b')) // b left over from previous
    assert(reader.read(array) === -1)
  }

  test("read(cbuf, offset, length)") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    val array = new Array[Char](5)
    assert(reader.read(array, 2, 3) === 3)
    assert(array === Array('\u0000', '\u0000', 'a', 'b', 'c'))
    assert(reader.read(array) === -1)
  }

  test("ready()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assert(reader.ready())
  }

  test("skip()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assert(reader.skip(2) === 2)
    assert(reader.read() === 'c')
    assert(reader.read() === -1)
    assert(reader.skip(2) === 0)
  }

  test("mark() and reset()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assert(reader.markSupported())
    assert(reader.read() === 'a')

    reader.mark(5)
    assert(reader.read() === 'b')
    assert(reader.read() === 'c')

    reader.reset()
    assert(reader.read() === 'b')

    reader.reset()
    assert(reader.read() === 'b')
  }

  test("close()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    (0 until 10).foreach { _ =>
      Using.resource(reader) { r =>
        assert(r.read() === 'a')
        r.skip(100)
        assert(r.read() === -1)
      }
    }
  }
}
