/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.eval.util

import java.io.InputStream

import akka.util.ByteString

/**
  * Wraps an akka [[ByteString]] as an input stream. This is mostly used to allow
  * the data to be read from other libraries like jackson.
  */
class ByteStringInputStream(buf: ByteString) extends InputStream {
  private[this] val n = buf.size
  private[this] var pos: Int = 0

  override def read(): Int = {
    if (pos >= n) -1
    else {
      val i = pos
      pos += 1
      buf(i)
    }
  }

  override def read(dst: Array[Byte], offset: Int, length: Int): Int = {
    pos match {
      case i if i >= n => -1
      case i if i == 0 => copy(buf, dst, offset, length)
      case i           => copy(buf.slice(i, n), dst, offset, length)
    }
  }

  @inline private def copy(src: ByteString, dst: Array[Byte], offset: Int, length: Int): Int = {
    src.copyToArray(dst, offset, length)
    val amount = math.min(n - pos, length)
    pos += amount
    amount
  }

  override def available(): Int = n - pos
}
