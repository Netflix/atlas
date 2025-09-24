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
import com.netflix.atlas.core.util.CharBufferReader
import com.netflix.atlas.core.util.SortedTagMap
import org.postgresql.copy.CopyManager

import java.io.Reader
import java.nio.CharBuffer

/**
  * Copy buffer that stores the data in the [text format].
  *
  * [text format]: https://www.postgresql.org/docs/13/sql-copy.html#id-1.9.3.55.9.2
  *
  * @param size
  *     Size of the underlying character buffer. Must be at least 1.
  * @param shouldEscapeValues
  *     True if string values should be escaped before being written to the buffer. For
  *     sources where the values are known not to include any special characters this
  *     can be disabled to improve performance.
  */
class TextCopyBuffer(size: Int, shouldEscapeValues: Boolean = true) extends CopyBuffer {

  require(size >= 1, "buffer size must be at least 1")

  private val data = CharBuffer.allocate(size)
  private var numRows = 0

  override def putId(id: ItemId): CopyBuffer = {
    putString(id.toString)
  }

  private def put(c: Char): TextCopyBuffer = {
    if (data.remaining() >= 1) {
      data.append(c)
    } else {
      data.limit(data.capacity())
    }
    this
  }

  private def put(str: String): TextCopyBuffer = {
    if (data.remaining() >= str.length) {
      data.append(str)
    } else {
      data.limit(data.capacity())
    }
    this
  }

  private def escapeAndPut(str: String): TextCopyBuffer = {
    if (shouldEscapeValues) {
      val n = str.length
      var i = 0
      while (i < n) {
        str.charAt(i) match {
          case '\b' => put("\\b")
          case '\f' => put("\\f")
          case '\n' => put("\\n")
          case '\r' => put("\\r")
          case '\t' => put("\\t")
          case 0x0B => put("\\v")
          case '"'  => put("\\\"")
          case '\\' => put("\\\\")
          case c    => put(c)
        }
        i += 1
      }
      this
    } else {
      put(str)
    }
  }

  private def putQuotedString(str: String): TextCopyBuffer = {
    put('"').escapeAndPut(str).put('"')
  }

  private def putKeyValue(k: String, sep: String, v: String): TextCopyBuffer = {
    putQuotedString(k).put(sep).putQuotedString(v)
  }

  override def putString(str: String): CopyBuffer = {
    if (str == null)
      put("\\N\t")
    else
      escapeAndPut(str).put('\t')
  }

  @scala.annotation.tailrec
  private def putJson(tags: SortedTagMap, i: Int): TextCopyBuffer = {
    if (i < tags.size) {
      put(',').putKeyValue(tags.key(i), ":", tags.value(i)).putJson(tags, i + 1)
    } else {
      this
    }
  }

  override def putTagsJson(tags: SortedTagMap): CopyBuffer = {
    if (tags.nonEmpty) {
      put('{').putKeyValue(tags.key(0), ":", tags.value(0)).putJson(tags, 1).put("}\t")
    } else {
      put("{}\t")
    }
  }

  override def putTagsJsonb(tags: SortedTagMap): CopyBuffer = {
    putTagsJson(tags)
  }

  @scala.annotation.tailrec
  private def putHstore(tags: SortedTagMap, i: Int): TextCopyBuffer = {
    if (i < tags.size) {
      put(',').putKeyValue(tags.key(i), "=>", tags.value(i)).putHstore(tags, i + 1)
    } else {
      this
    }
  }

  override def putTagsHstore(tags: SortedTagMap): CopyBuffer = {
    if (tags.nonEmpty) {
      putKeyValue(tags.key(0), "=>", tags.value(0)).putHstore(tags, 1).put('\t')
    } else {
      put('\t')
    }
  }

  override def putTagsText(tags: SortedTagMap): CopyBuffer = {
    putTagsJson(tags)
  }

  override def putShort(value: Short): CopyBuffer = {
    putString(value.toString)
  }

  override def putInt(value: Int): CopyBuffer = {
    putString(value.toString)
  }

  override def putLong(value: Long): CopyBuffer = {
    putString(value.toString)
  }

  override def putDouble(value: Double): CopyBuffer = {
    putString(value.toString)
  }

  @scala.annotation.tailrec
  private def putDoubleArray(values: Array[Double], i: Int): TextCopyBuffer = {
    if (i < values.length) {
      put(',').put(values(i).toString).putDoubleArray(values, i + 1)
    } else {
      this
    }
  }

  override def putDoubleArray(values: Array[Double]): CopyBuffer = {
    if (values.length > 0) {
      put('{').put(values(0).toString).putDoubleArray(values, 1).put("}\t")
    } else {
      put("{}\t")
    }
  }

  override def nextRow(): Boolean = {
    val i = data.position() - 1
    if (i < 0 || data.get(i) == '\n' || (data.get(i) != '\t' && numRows == 0)) {
      throw new IllegalStateException(
        "nextRow() called on empty row, usually means a row is too big to fit"
      )
    } else if (data.get(i) == '\t') {
      numRows += 1
      data.position(i)
      data.put('\n')
      data.mark()
      true
    } else {
      // partial row written
      false
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
    data.clear()
    numRows = 0
  }

  def reader(): Reader = {
    // Buffer is marked for each new row, reset is used to ignore a partial row that
    // could not fit in the buffer.
    data.reset().flip()
    new CharBufferReader(data)
  }

  override def copyIn(copyManager: CopyManager, table: String): Unit = {
    val copySql = s"copy $table from stdin (format text)"
    copyManager.copyIn(copySql, reader())
  }

  override def toString: String = {
    new String(data.array(), 0, data.position())
  }
}
