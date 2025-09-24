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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.util.Using

class ByteBufferInputStreamSuite extends FunSuite {

  private def wrap(str: String): ByteBuffer = {
    ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8))
  }

  test("read()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assertEquals(in.read(), 'a'.toInt)
    assertEquals(in.read(), 'b'.toInt)
    assertEquals(in.read(), 'c'.toInt)
    assertEquals(in.read(), -1)
  }

  test("read(buf)") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    val array = new Array[Byte](2)
    assertEquals(in.read(array), 2)
    assertEquals(array.toSeq, Array[Byte]('a', 'b').toSeq)
    assertEquals(in.read(array), 1)
    assertEquals(array.toSeq, Array[Byte]('c', 'b').toSeq) // b left over from previous
    assertEquals(in.read(array), -1)
  }

  test("read(buf, offset, length)") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    val array = new Array[Byte](5)
    assertEquals(in.read(array, 2, 3), 3)
    assertEquals(array.toSeq, Array[Byte]('\u0000', '\u0000', 'a', 'b', 'c').toSeq)
    assertEquals(in.read(array), -1)
  }

  test("available()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assertEquals(in.available(), 3)
    assertEquals(in.skip(2), 2L)
    assertEquals(in.available(), 1)
    assertEquals(in.read(), 'c'.toInt)
    assertEquals(in.available(), 0)
  }

  test("skip()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assertEquals(in.skip(2), 2L)
    assertEquals(in.read(), 'c'.toInt)
    assertEquals(in.read(), -1)
    assertEquals(in.skip(2), 0L)
  }

  test("mark() and reset()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    assert(in.markSupported())
    assertEquals(in.read(), 'a'.toInt)

    in.mark(5)
    assertEquals(in.read(), 'b'.toInt)
    assertEquals(in.read(), 'c'.toInt)

    in.reset()
    assertEquals(in.read(), 'b'.toInt)

    in.reset()
    assertEquals(in.read(), 'b'.toInt)
  }

  test("close()") {
    val buffer = wrap("abc")
    val in = new ByteBufferInputStream(buffer)
    (0 until 10).foreach { _ =>
      Using.resource(in) { r =>
        assertEquals(r.read(), 'a'.toInt)
        r.skip(100)
        assertEquals(r.read(), -1)
      }
    }
  }
}
