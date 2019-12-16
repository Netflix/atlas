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

import java.io.ByteArrayInputStream
import java.util.Random

import akka.util.ByteString
import org.scalatest.funsuite.AnyFunSuite

class ByteStringInputStreamSuite extends AnyFunSuite {
  test("read() and available()") {
    val random = new Random(42)
    val data = new Array[Byte](4096)
    random.nextBytes(data)

    val bais = new ByteArrayInputStream(data)
    val bsis = new ByteStringInputStream(ByteString(data))

    data.indices.foreach { i =>
      assert(bais.available() === data.length - i)
      assert(bsis.available() === data.length - i)
      assert(bais.read() === bsis.read())
    }

    assert(bais.read() === bsis.read())
  }

  test("read(buffer, offset, length)") {
    val random = new Random(42)
    val data = new Array[Byte](4096)
    random.nextBytes(data)

    val bais = new ByteArrayInputStream(data)
    val bsis = new ByteStringInputStream(ByteString(data))

    val b1 = new Array[Byte](13)
    val b2 = new Array[Byte](13)
    var i = 0
    while (i < data.length) {
      val len1 = bais.read(b1)
      val len2 = bsis.read(b2)
      assert(len1 === len2)
      assert(b1 === b2)
      i += len2
    }

    assert(bais.read(b1) === bsis.read(b2))
  }
}
