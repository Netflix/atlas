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

import java.nio.CharBuffer
import scala.util.Using

class CharBufferReaderSuite extends FunSuite {

  test("read()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assertEquals(reader.read(), 'a'.toInt)
    assertEquals(reader.read(), 'b'.toInt)
    assertEquals(reader.read(), 'c'.toInt)
    assertEquals(reader.read(), -1)
  }

  test("read(cbuf)") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    val array = new Array[Char](2)
    assertEquals(reader.read(array), 2)
    assertEquals(array.toSeq, Array('a', 'b').toSeq)
    assertEquals(reader.read(array), 1)
    assertEquals(array.toSeq, Array('c', 'b').toSeq) // b left over from previous
    assertEquals(reader.read(array), -1)
  }

  test("read(cbuf, offset, length)") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    val array = new Array[Char](5)
    assertEquals(reader.read(array, 2, 3), 3)
    assertEquals(array.toSeq, Array('\u0000', '\u0000', 'a', 'b', 'c').toSeq)
    assertEquals(reader.read(array), -1)
  }

  test("ready()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assert(reader.ready())
  }

  test("skip()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assertEquals(reader.skip(2), 2L)
    assertEquals(reader.read(), 'c'.toInt)
    assertEquals(reader.read(), -1)
    assertEquals(reader.skip(2), 0L)
  }

  test("mark() and reset()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    assert(reader.markSupported())
    assertEquals(reader.read(), 'a'.toInt)

    reader.mark(5)
    assertEquals(reader.read(), 'b'.toInt)
    assertEquals(reader.read(), 'c'.toInt)

    reader.reset()
    assertEquals(reader.read(), 'b'.toInt)

    reader.reset()
    assertEquals(reader.read(), 'b'.toInt)
  }

  test("close()") {
    val buffer = CharBuffer.wrap("abc")
    val reader = new CharBufferReader(buffer)
    (0 until 10).foreach { _ =>
      Using.resource(reader) { r =>
        assertEquals(r.read(), 'a'.toInt)
        r.skip(100)
        assertEquals(r.read(), -1)
      }
    }
  }
}
