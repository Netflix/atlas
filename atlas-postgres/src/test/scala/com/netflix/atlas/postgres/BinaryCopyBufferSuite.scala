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
import com.netflix.atlas.core.util.Strings
import munit.FunSuite

import java.io.InputStream

class BinaryCopyBufferSuite extends FunSuite {

  test("too small") {
    intercept[IllegalArgumentException] {
      new BinaryCopyBuffer(26, 1)
    }
  }

  test("not enough fields") {
    intercept[IllegalArgumentException] {
      new BinaryCopyBuffer(27, 0)
    }
  }

  test("avoid endless loop") {
    val buffer = new BinaryCopyBuffer(27, 1)
    assert(buffer.putString("").nextRow())
    buffer.clear()
    intercept[IllegalStateException] {
      buffer.putString("foo").nextRow()
    }
  }

  test("putId") {
    val buffer = new BinaryCopyBuffer(100, 1)
    val id = ItemIdCalculator.compute(SortedTagMap("a" -> "1"))
    buffer.putId(id)
    assertEquals(buffer.toString, s"\\x00\\x01\\x00\\x00\\x00($id")
  }

  test("putString") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putString("foo")
    assertEquals(buffer.toString, s"\\x00\\x01\\x00\\x00\\x00\\x03foo")
  }

  test("putString null") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putString(null)
    assertEquals(buffer.toString, s"\\x00\\x01\\xff\\xff\\xff\\xff")
  }

  test("putString escape") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putString("\b\f\n\r\t\u000b\"\\")
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x08\\x08\\x0c\\x0a\\x0d\\x09\\x0b\"\\"
    )
  }

  test("putString not enough space") {
    val buffer = new BinaryCopyBuffer(28, 1)
    buffer.putString("foo")
    assert(!buffer.hasRemaining)
  }

  test("putTagsJson") {
    val buffer = new BinaryCopyBuffer(100, 1)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsJson(tags)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x11{\"a\":\"1\",\"b\":\"2\"}")
  }

  test("putTagsJson empty") {
    val buffer = new BinaryCopyBuffer(100, 1)
    val tags = SortedTagMap.empty
    buffer.putTagsJson(tags)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x02{}")
  }

  test("putTagsJsonb") {
    val buffer = new BinaryCopyBuffer(100, 1)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsJsonb(tags)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x12\\x01{\"a\":\"1\",\"b\":\"2\"}")
  }

  test("putTagsHstore") {
    val buffer = new BinaryCopyBuffer(100, 1)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsHstore(tags)
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x18\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x01a\\x00\\x00\\x00\\x011\\x00\\x00\\x00\\x01b\\x00\\x00\\x00\\x012"
    )
  }

  test("putTagsHstore empty") {
    val buffer = new BinaryCopyBuffer(100, 1)
    val tags = SortedTagMap.empty
    buffer.putTagsHstore(tags)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x00")
  }

  test("putTagsText") {
    val buffer = new BinaryCopyBuffer(100, 1)
    val tags = SortedTagMap("a" -> "1", "b" -> "2")
    buffer.putTagsText(tags)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x11{\"a\":\"1\",\"b\":\"2\"}")
  }

  test("putShort") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putShort(42)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x02\\x00*")
  }

  test("putInt") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putInt(42)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x04\\x00\\x00\\x00*")
  }

  test("putLong") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putLong(42L)
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x08\\x00\\x00\\x00\\x00\\x00\\x00\\x00*"
    )
  }

  test("putDouble") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putDouble(42.0)
    assertEquals(buffer.toString, "\\x00\\x01\\x00\\x00\\x00\\x08@E\\x00\\x00\\x00\\x00\\x00\\x00")
  }

  test("putDouble NaN") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putDouble(Double.NaN)
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x08\\x7f\\xf8\\x00\\x00\\x00\\x00\\x00\\x00"
    )
  }

  test("putDouble Infinity") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putDouble(Double.PositiveInfinity)
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x08\\x7f\\xf0\\x00\\x00\\x00\\x00\\x00\\x00"
    )
  }

  test("putDouble -Infinity") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putDouble(Double.NegativeInfinity)
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x08\\xff\\xf0\\x00\\x00\\x00\\x00\\x00\\x00"
    )
  }

  test("putDoubleArray") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putDoubleArray(Array.empty).putDoubleArray(Array(1.0, 1.5, 2.0, 2.5))
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x14\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\xbd\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
    )
  }

  test("nextRow") {
    val buffer = new BinaryCopyBuffer(100, 1)
    buffer.putString("foo").putString("bar").nextRow()
    assertEquals(
      buffer.toString,
      "\\x00\\x01\\x00\\x00\\x00\\x03foo\\x00\\x00\\x00\\x03bar\\x00\\x01"
    )
  }

  test("nextRow on empty row") {
    val buffer = new BinaryCopyBuffer(100, 1)
    intercept[IllegalStateException] {
      buffer.nextRow()
    }
    buffer.putInt(0).nextRow()
    intercept[IllegalStateException] {
      buffer.nextRow()
    }
  }

  private def toString(in: InputStream): String = {
    val builder = new java.lang.StringBuilder
    val buf = new Array[Byte](128)
    var length = in.read(buf)
    while (length > 0) {
      var i = 0
      while (i < length) {
        val b = buf(i)
        if (b >= ' ' && b <= '~') {
          builder.append(b.asInstanceOf[Char])
        } else {
          val ub = b & 0xFF
          builder.append("\\x").append(Strings.zeroPad(ub, 2))
        }
        i += 1
      }
      length = in.read(buf)
    }
    builder.toString()
  }

  test("inputStream") {
    val buffer = new BinaryCopyBuffer(100, 2)
    buffer.putInt(0).putString("foo").nextRow()
    assertEquals(
      toString(buffer.inputStream()),
      "PGCOPY\\x0a\\xff\\x0d\\x0a\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x03foo\\xff\\xff"
    )
  }

  test("inputStream with partial row") {
    val buffer = new BinaryCopyBuffer(40, 2)
    assert(buffer.putInt(0).putString("foo").nextRow())
    assert(!buffer.putInt(1).putString("bar").nextRow())
    assertEquals(
      toString(buffer.inputStream()),
      "PGCOPY\\x0a\\xff\\x0d\\x0a\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x03foo\\xff\\xff"
    )
  }

  test("remaining") {
    val buffer = new BinaryCopyBuffer(36, 2)
    buffer.putInt(2)
    assert(buffer.hasRemaining)
    assertEquals(buffer.remaining, 7)

    buffer.putString("foo")
    assert(!buffer.hasRemaining)
    assertEquals(buffer.remaining, 0)
  }

  test("rows") {
    val buffer = new BinaryCopyBuffer(100, 1)
    var i = 0
    while (buffer.putInt(i).nextRow()) {
      i = i + 1
      assertEquals(buffer.rows, i)
    }
    assertEquals(buffer.rows, i)
  }

  test("clear") {
    val buffer = new BinaryCopyBuffer(100, 2)
    buffer.putInt(0).putString("foo").nextRow()
    assertEquals(
      toString(buffer.inputStream()),
      "PGCOPY\\x0a\\xff\\x0d\\x0a\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x03foo\\xff\\xff"
    )
    buffer.clear()
    buffer.putInt(1).putString("bar").nextRow()
    assertEquals(
      toString(buffer.inputStream()),
      "PGCOPY\\x0a\\xff\\x0d\\x0a\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x03bar\\xff\\xff"
    )
  }
}
