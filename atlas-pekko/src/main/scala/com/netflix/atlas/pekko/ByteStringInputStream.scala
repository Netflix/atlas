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
package com.netflix.atlas.pekko

import java.io.InputStream
import org.apache.pekko.util.ByteString

import java.util.zip.GZIPInputStream

/**
  * Wraps a `ByteString` to allow it to be read from code expecting an `InputStream`. This
  * can be used to avoid allocating a temporary array and using `ByteArrayInputStream`.
  */
class ByteStringInputStream(data: ByteString) extends InputStream {

  private val buffers = data.asByteBuffers.iterator
  private var current = buffers.next()

  private def nextBuffer(): Unit = {
    if (!current.hasRemaining && buffers.hasNext) {
      current = buffers.next()
    }
  }

  override def read(): Int = {
    nextBuffer()
    if (current.hasRemaining) current.get() & 255 else -1
  }

  override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    nextBuffer()
    val amount = math.min(current.remaining(), length)
    if (amount == 0) -1
    else {
      current.get(bytes, offset, amount)
      amount
    }
  }

  override def available(): Int = {
    nextBuffer()
    current.remaining()
  }
}

object ByteStringInputStream {

  // Magic header to recognize GZIP compressed data
  // http://www.zlib.org/rfc-gzip.html#file-format
  private val gzipMagicHeader = ByteString(Array(0x1F.toByte, 0x8B.toByte))

  /**
    * Create an InputStream for reading the content of the ByteString. If the data is
    * gzip compressed, then it will be wrapped in a GZIPInputStream to handle the
    * decompression of the data. This can be handled at the server layer, but it may
    * be preferable to decompress while parsing into the final object model to reduce
    * the need to allocate an intermediate ByteString of the uncompressed data.
    */
  def create(data: ByteString): InputStream = {
    if (data.startsWith(gzipMagicHeader))
      new GZIPInputStream(new ByteStringInputStream(data))
    else
      new ByteStringInputStream(data)
  }
}
