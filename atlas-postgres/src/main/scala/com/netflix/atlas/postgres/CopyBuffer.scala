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
import com.netflix.atlas.core.util.SortedTagMap
import org.postgresql.copy.CopyManager

/**
  * Buffer for storing data in memory in a format that can be passed to the Postgres COPY
  * operation. The caller is responsible for knowing the table format and putting values
  * in the expected order. Typical usage:
  *
  * ```
  * val buffer = ...
  * rows.foreach { row =>
  *   buffer.putString(row.column(1)).putInt(row.column(2))
  *   if (!buffer.nextRow()) {
  *     buffer.copyIn(copyManager, tableName)
  *     buffer.putString(row.column(1)).putInt(row.column(2)).nextRow()
  *   }
  * }
  * buffer.copyIn(copyManager, tableName)
  * ```
  */
trait CopyBuffer {

  /** Put an id column value into the buffer. */
  def putId(id: ItemId): CopyBuffer

  /** Put a string column value into the buffer. */
  def putString(str: String): CopyBuffer

  /** Put a JSON column value into the buffer. */
  def putTagsJson(tags: SortedTagMap): CopyBuffer

  /** Put a JSONB column value into the buffer. */
  def putTagsJsonb(tags: SortedTagMap): CopyBuffer

  /** Put an HSTORE column value into the buffer. */
  def putTagsHstore(tags: SortedTagMap): CopyBuffer

  /** Put a JSON string column value as text into the buffer. */
  def putTagsText(tags: SortedTagMap): CopyBuffer

  /** Put a signed 2-byte integer column value into the buffer. */
  def putShort(value: Short): CopyBuffer

  /** Put a signed 4-byte integer column value into the buffer. */
  def putInt(value: Int): CopyBuffer

  /** Put a signed 8-byte integer column value into the buffer. */
  def putLong(value: Long): CopyBuffer

  /** Put an 8-byte floating point column value into the buffer. */
  def putDouble(value: Double): CopyBuffer

  /** Put an array of 8-byte floating point column value into the buffer. */
  def putDoubleArray(values: Array[Double]): CopyBuffer

  /**
    * Indicate the end of the row and prepare for the next. Returns true if the row
    * was able to fit in the buffer. Otherwise the buffer should be copied to Postgres
    * and cleared before re-adding the row.
    *
    * If a single row is too big to fit in the buffer, then an IllegalStateException
    * will be thrown to avoid an endless loop.
    */
  def nextRow(): Boolean

  /** Returns true if there is space remaining in the buffer. */
  def hasRemaining: Boolean

  /** Returns the number of bytes remaining in the buffer. */
  def remaining: Int

  /** Returns the number of completed rows that are in the buffer. */
  def rows: Int

  /** Clear the contents of the buffer so it can be reused. */
  def clear(): Unit

  /** Copy the data in this buffer into the specified table. */
  def copyIn(copyManager: CopyManager, table: String): Unit
}
