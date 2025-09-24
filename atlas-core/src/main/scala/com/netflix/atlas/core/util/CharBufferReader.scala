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

import java.io.Reader
import java.nio.CharBuffer

/**
  * Wraps a CharBuffer so it can be used with interfaces that require a Reader. The buffer
  * should not be modified outside of the reader until reading is complete.
  */
class CharBufferReader(buffer: CharBuffer) extends Reader {

  override def read(cbuf: Array[Char], offset: Int, length: Int): Int = {
    if (buffer.hasRemaining) {
      val readLength = math.min(buffer.remaining(), length)
      buffer.get(cbuf, offset, readLength)
      readLength
    } else {
      -1
    }
  }

  override def read(): Int = {
    if (buffer.hasRemaining) buffer.get() else -1
  }

  override def ready(): Boolean = true

  override def skip(n: Long): Long = {
    val skipAmount = math.min(buffer.remaining(), n).toInt
    buffer.position(buffer.position() + skipAmount)
    skipAmount
  }

  override def markSupported(): Boolean = true

  override def mark(readAheadLimit: Int): Unit = {
    buffer.mark()
  }

  override def reset(): Unit = {
    buffer.reset()
  }

  override def close(): Unit = {
    buffer.flip()
  }
}
