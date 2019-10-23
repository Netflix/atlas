/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.akka

import java.io.InputStream

import akka.util.ByteString

/**
  * Wraps a `ByteString` to allow it to be read from code expecting an `InputStream`. This
  * can be used to avoid allocating a temporary array and using `ByteArrayInputStream`.
  */
class ByteStringInputStream(data: ByteString) extends InputStream {
  private var pos = -1
  private val length = data.length

  override def read(): Int = {
    pos += 1
    if (pos >= length) -1 else data(pos) & 255
  }

  override def available(): Int = {
    if (pos >= length) 0 else length - pos - 1
  }
}
