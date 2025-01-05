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
package com.netflix.atlas.postgres

import com.netflix.atlas.core.model.ItemIdCalculator
import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

import java.io.Reader

class TextCopyBufferSuite extends FunSuite {

  test("too small") {
    intercept[IllegalArgumentException] {
      new TextCopyBuffer(0)
    }
  }

  test("avoid endless loop") {
    val buffer = new TextCopyBuffer(1)
    assert(buffer.putString("").nextRow())
    buffer.clear()
    intercept[IllegalStateException] {
      buffer.putString("foo").nextRow()
    }
  }

  test("putId") {
    val buffer = new TextCopyBuffer(100)
    val id = ItemIdCalculator.compute(SortedTagMap("a" -> "1"))
    buffer.putId(id)
    assertEquals(buffer.toString, s"$id\t")
  }

  test("putString") {
    val buffer = new TextCopyBuffer(100)
    buffer.putString("foo")
    assertEquals(buffer.toString, "foo\t")
  }

  test("putString null") {
    val buffer = new TextCopyBuffer(100)
    buffer.putString(null)
    assertEquals(buffer.toString, "\\N\t")
  }

  test("putString escape") {
    val buffer = new TextCopyBuffer(100)
    buffer.putString("\b\f\n\r\t\u000b\"\\")
    assertEquals(buffer.toString, "\\b\\f\\n\\r\\t\\v\\\"\\\\\t")
  }

  test("putString escape disabled") {
    val buffer = new TextCopyBuffer(100, false)
    buffer.putString("\b\f\n\r\t\u000b\"\\")
    assertEquals(buffer.toString, "\b\f\n\r\t\u000b\"\\\t")
  }

  test("putString not enough space") {
    val buffer = new TextCopyBuffer(2)
    buffer.putString("foo")
    assert(!buffer.hasRemaining)
  }

  test("putString not enough space after escaping") {
    val buffer = new TextCopyBuffer(6)
    buffer.putString("foo\n\t")
    assert(!buffer.hasRemaining)
  }

  test("putTagsJson") {
    val buffer = new TextCopyBuffer(100)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsJson(tags)
    assertEquals(buffer.toString, "{\"a\":\"1\",\"b\":\"2\"}\t")
  }

  test("putTagsJson empty") {
    val buffer = new TextCopyBuffer(100)
    val tags = SortedTagMap.empty
    buffer.putTagsJson(tags)
    assertEquals(buffer.toString, "{}\t")
  }

  test("putTagsJsonb") {
    val buffer = new TextCopyBuffer(100)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsJsonb(tags)
    assertEquals(buffer.toString, "{\"a\":\"1\",\"b\":\"2\"}\t")
  }

  test("putTagsHstore") {
    val buffer = new TextCopyBuffer(100)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsHstore(tags)
    assertEquals(buffer.toString, "\"a\"=>\"1\",\"b\"=>\"2\"\t")
  }

  test("putTagsHstore empty") {
    val buffer = new TextCopyBuffer(100)
    val tags = SortedTagMap.empty
    buffer.putTagsHstore(tags)
    assertEquals(buffer.toString, "\t")
  }

  test("putTagsText") {
    val buffer = new TextCopyBuffer(100)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsText(tags)
    assertEquals(buffer.toString, "{\"a\":\"1\",\"b\":\"2\"}\t")
  }

  test("putShort") {
    val buffer = new TextCopyBuffer(100)
    buffer.putShort(42)
    assertEquals(buffer.toString, "42\t")
  }

  test("putInt") {
    val buffer = new TextCopyBuffer(100)
    buffer.putInt(42)
    assertEquals(buffer.toString, "42\t")
  }

  test("putLong") {
    val buffer = new TextCopyBuffer(100)
    buffer.putLong(42L)
    assertEquals(buffer.toString, "42\t")
  }

  test("putDouble") {
    val buffer = new TextCopyBuffer(100)
    buffer.putDouble(42.0)
    assertEquals(buffer.toString, "42.0\t")
  }

  test("putDouble NaN") {
    val buffer = new TextCopyBuffer(100)
    buffer.putDouble(Double.NaN)
    assertEquals(buffer.toString, "NaN\t")
  }

  test("putDouble Infinity") {
    val buffer = new TextCopyBuffer(100)
    buffer.putDouble(Double.PositiveInfinity)
    assertEquals(buffer.toString, "Infinity\t")
  }

  test("putDouble -Infinity") {
    val buffer = new TextCopyBuffer(100)
    buffer.putDouble(Double.NegativeInfinity)
    assertEquals(buffer.toString, "-Infinity\t")
  }

  test("putDoubleArray") {
    val buffer = new TextCopyBuffer(100)
    buffer.putDoubleArray(Array.empty).putDoubleArray(Array(1.0, 1.5, 2.0, 2.5))
    assertEquals(buffer.toString, "{}\t{1.0,1.5,2.0,2.5}\t")
  }

  test("nextRow") {
    val buffer = new TextCopyBuffer(100)
    buffer.putString("foo").putString("bar").nextRow()
    assertEquals(buffer.toString, "foo\tbar\n")
  }

  test("nextRow on empty row") {
    val buffer = new TextCopyBuffer(100)
    intercept[IllegalStateException] {
      buffer.nextRow()
    }
    buffer.putInt(0).nextRow()
    intercept[IllegalStateException] {
      buffer.nextRow()
    }
  }

  private def toString(reader: Reader): String = {
    val builder = new java.lang.StringBuilder
    val buf = new Array[Char](128)
    var length = reader.read(buf)
    while (length > 0) {
      builder.append(buf, 0, length)
      length = reader.read(buf)
    }
    builder.toString
  }

  test("reader") {
    val buffer = new TextCopyBuffer(100)
    buffer.putInt(0).putString("foo").nextRow()
    assertEquals(toString(buffer.reader()), "0\tfoo\n")
  }

  test("reader with partial row") {
    val buffer = new TextCopyBuffer(9)
    assert(buffer.putInt(0).putString("foo").nextRow())
    assert(!buffer.putInt(1).putString("bar").nextRow())
    assertEquals(toString(buffer.reader()), "0\tfoo\n")
  }

  test("remaining") {
    val buffer = new TextCopyBuffer(4)
    buffer.putInt(2)
    assert(buffer.hasRemaining)
    assertEquals(buffer.remaining, 2)

    buffer.putString("foo")
    assert(!buffer.hasRemaining)
    assertEquals(buffer.remaining, 0)
  }

  test("rows") {
    val buffer = new TextCopyBuffer(100)
    var i = 0
    while (buffer.putInt(i).nextRow()) {
      i = i + 1
      assertEquals(buffer.rows, i)
    }
    assertEquals(buffer.rows, i)
  }

  test("clear") {
    val buffer = new TextCopyBuffer(100)
    buffer.putInt(0).putString("foo").nextRow()
    assertEquals(toString(buffer.reader()), "0\tfoo\n")
    buffer.clear()
    buffer.putInt(1).putString("bar").nextRow()
    assertEquals(toString(buffer.reader()), "1\tbar\n")
  }
}
