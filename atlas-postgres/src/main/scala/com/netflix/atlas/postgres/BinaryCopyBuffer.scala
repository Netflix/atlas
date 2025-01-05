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

import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.util.ByteBufferInputStream
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.atlas.core.util.Strings
import org.postgresql.copy.CopyManager

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CoderResult
import java.nio.charset.StandardCharsets

/**
  * Copy buffer that stores the data in the [binary format].
  *
  * [binary format] https://www.postgresql.org/docs/13/sql-copy.html#id-1.9.3.55.9.4
  *
  * @param size
  *     Size of the underlying byte buffer. Must be at least 27 bytes for headers
  *     and footers.
  * @param numFields
  *     Number of fields per tuple.
  */
class BinaryCopyBuffer(size: Int, numFields: Short) extends CopyBuffer {

  // Binary format has overhead for header (19 bytes) and footer (2 bytes). Each tuple
  // starts with number of fields (2 bytes). A single value needs a length (4 bytes), but
  // the value can have zero length or be null. So min size to hold a complete entry with
  // a single empty value is 27 bytes.
  require(size >= 27, "buffer size must be at least 27")
  require(numFields > 0, "number of fields per tuple must be greater than 0")

  private val data = ByteBuffer.allocate(size)
  private var numRows = 0
  private var rowStartPosition = 0
  clear()

  private var incompleteWrite = false

  private val stringBuilder = new java.lang.StringBuilder
  private val encoder = StandardCharsets.UTF_8.newEncoder()

  private def hasSpace(n: Int): Boolean = {
    // include 2 bytes needed for the footer
    data.remaining() >= n + 2
  }

  private def putS(value: Short): BinaryCopyBuffer = {
    if (hasSpace(2))
      data.putShort(value)
    else
      incompleteWrite = true
    this
  }

  private def putI(value: Int): BinaryCopyBuffer = {
    if (hasSpace(4))
      data.putInt(value)
    else
      incompleteWrite = true
    this
  }

  private def putI(index: Int, value: Int): BinaryCopyBuffer = {
    if (data.limit() - index >= 4)
      data.putInt(index, value)
    else
      incompleteWrite = true
    this
  }

  override def putId(id: ItemId): CopyBuffer = {
    putString(id.toString)
  }

  private def encodeString(str: CharBuffer): Unit = {
    encoder.reset()
    if (encoder.encode(str, data, true) == CoderResult.OVERFLOW) {
      incompleteWrite = true
    }
  }

  private def putString(str: CharBuffer): BinaryCopyBuffer = {
    if (str == null) {
      putI(-1)
    } else {
      val valueSizePos = data.position()
      putI(0) // placeholder for size
      encodeString(str)
      val valueSize = data.position() - valueSizePos - 4
      putI(valueSizePos, valueSize)
      this
    }
  }

  override def putString(str: String): CopyBuffer = {
    if (str == null) {
      putI(-1)
    } else {
      putString(CharBuffer.wrap(str))
    }
  }

  private def toJson(tags: SortedTagMap): CharBuffer = {
    if (tags.isEmpty) {
      CharBuffer.wrap("{}")
    } else {
      stringBuilder.setLength(0)
      stringBuilder.append("{\"").append(tags.key(0)).append("\":\"").append(tags.value(0))
      var i = 1
      while (i < tags.size) {
        stringBuilder.append("\",\"").append(tags.key(i)).append("\":\"").append(tags.value(i))
        i += 1
      }
      stringBuilder.append("\"}")
      CharBuffer.wrap(stringBuilder)
    }
  }

  override def putTagsJson(tags: SortedTagMap): CopyBuffer = {
    putString(toJson(tags))
  }

  override def putTagsJsonb(tags: SortedTagMap): CopyBuffer = {
    val valueSizePos = data.position()
    putI(0) // placeholder for size
    if (hasSpace(1))
      data.put(1.toByte)
    else
      incompleteWrite = true
    encodeString(toJson(tags))
    val valueSize = data.position() - valueSizePos - 4
    putI(valueSizePos, valueSize)
    this
  }

  override def putTagsHstore(tags: SortedTagMap): CopyBuffer = {
    val valueSizePos = data.position()
    putI(0) // placeholder for value size

    putI(tags.size)
    var i = 0
    while (i < tags.size) {
      putString(tags.key(i))
      putString(tags.value(i))
      i += 1
    }

    val valueSize = data.position() - valueSizePos - 4
    putI(valueSizePos, valueSize)
    this
  }

  override def putTagsText(tags: SortedTagMap): CopyBuffer = {
    putTagsJson(tags)
  }

  override def putShort(value: Short): CopyBuffer = {
    putI(2).putS(value)
  }

  override def putInt(value: Int): CopyBuffer = {
    putI(4).putI(value)
  }

  override def putLong(value: Long): CopyBuffer = {
    if (hasSpace(16)) {
      data.putInt(8)
      data.putLong(value)
    } else {
      incompleteWrite = true
    }
    this
  }

  override def putDouble(value: Double): CopyBuffer = {
    if (hasSpace(16)) {
      data.putInt(8)
      data.putDouble(value)
    } else {
      incompleteWrite = true
    }
    this
  }

  override def putDoubleArray(values: Array[Double]): CopyBuffer = {
    val arraySize = 20 + 12 * values.length
    if (hasSpace(arraySize)) {
      data.putInt(arraySize) // Length of overall array value

      // Array header
      data.putInt(1) // num dimensions
      data.putInt(0) // does it contain nulls? always false for this use-case
      data.putInt(701) // element type (see `include/catalog/pg_type.dat`)
      data.putInt(values.length) // array length
      data.putInt(0) // lower bound for the dimension

      // Values
      var i = 0
      while (i < values.length) {
        data.putInt(8) // value length
        data.putDouble(values(i))
        i += 1
      }
    } else {
      incompleteWrite = true
    }
    this
  }

  override def nextRow(): Boolean = {
    val rowNonEmpty = data.position() > rowStartPosition
    if (!incompleteWrite && data.remaining() >= 2 && rowNonEmpty) {
      numRows += 1
      data.mark()
      putS(numFields)
      rowStartPosition = data.position()
      true
    } else if (numRows > 0 && rowNonEmpty) {
      false
    } else {
      throw new IllegalStateException(
        "nextRow() called on empty row, usually means a row is too big to fit"
      )
    }
  }

  override def hasRemaining: Boolean = {
    data.hasRemaining
  }

  override def remaining: Int = {
    data.remaining()
  }

  override def rows: Int = numRows

  override def clear(): Unit = {
    incompleteWrite = false
    data.clear()
    data.put(BinaryCopyBuffer.Signature)
    data.putInt(0) // flags
    data.putInt(0) // header extension
    numRows = -1 // will get incremented by 0 in next step
    rowStartPosition = 0
    nextRow() // prepare for first row
  }

  def inputStream(): InputStream = {
    // Buffer is marked for each new row, reset is used to ignore a partial row that
    // could not fit in the buffer.
    data.reset()
    data.putShort(-1) // footer
    data.flip()
    new ByteBufferInputStream(data)
  }

  override def copyIn(copyManager: CopyManager, table: String): Unit = {
    val copySql = s"copy $table from stdin (format binary)"
    copyManager.copyIn(copySql, inputStream())
  }

  override def toString: String = {
    val builder = new java.lang.StringBuilder
    val bytes = data.array()
    val n = data.position()
    var i = 19 // ignore header
    while (i < n) {
      val b = bytes(i)
      if (b >= ' ' && b <= '~') {
        builder.append(b.asInstanceOf[Char])
      } else {
        val ub = b & 0xFF
        builder.append("\\x").append(Strings.zeroPad(ub, 2))
      }
      i += 1
    }
    builder.toString
  }
}

object BinaryCopyBuffer {

  private val Signature: Array[Byte] =
    Array('P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xFF.toByte, '\r', '\n', 0)
}
