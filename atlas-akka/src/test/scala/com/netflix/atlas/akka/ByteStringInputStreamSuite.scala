/*
 * Copyright 2014-2020 Netflix, Inc.
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
import java.security.MessageDigest
import java.util.Random

import akka.util.ByteString
import org.scalatest.funsuite.AnyFunSuite

class ByteStringInputStreamSuite extends AnyFunSuite {

  private def singleRead(name: String, data: ByteString): Unit = {
    test(s"$name: read() and available()") {
      val bais = new ByteArrayInputStream(data.toArray)
      val bsis = new ByteStringInputStream(data)

      data.indices.foreach { i =>
        if (data.isCompact) {
          assert(bais.available() === data.length - i)
          assert(bsis.available() === data.length - i)
        } else {
          assert(bsis.available() > 0)
        }
        assert(bais.read() === bsis.read())
      }

      assert(bais.read() === bsis.read())
    }
  }

  private def bulkRead(name: String, data: ByteString): Unit = {
    test(s"$name: read(buffer, offset, length)") {
      val bais = new ByteArrayInputStream(data.toArray)
      val bsis = new ByteStringInputStream(data)

      val h1 = MessageDigest.getInstance("SHA-256")
      val h2 = MessageDigest.getInstance("SHA-256")

      val b1 = new Array[Byte](13)
      val b2 = new Array[Byte](13)
      var i = 0
      while (i < data.length) {
        val len1 = bais.read(b1)
        val len2 = bsis.read(b2)
        if (data.isCompact) {
          assert(len1 === len2)
          assert(b1 === b2)
        }
        if (len1 > 0) h1.update(b1, 0, len1)
        if (len2 > 0) h2.update(b2, 0, len2)
        i += len2
      }

      assert(bais.read(b1) === bsis.read(b2))
      assert(h1.digest() === h2.digest())
    }
  }

  private def compactByteString(n: Int): ByteString = {
    val random = new Random()
    val data = new Array[Byte](n)
    random.nextBytes(data)
    ByteString(data)
  }

  private def compositeByteString(n: Int, m: Int): ByteString = {
    val builder = ByteString.newBuilder
    (0 until n).foreach { _ =>
      builder.append(compactByteString(m))
    }
    builder.result()
  }

  singleRead("compact", compactByteString(4096))
  bulkRead("compact", compactByteString(4096))

  singleRead("composite small", compositeByteString(100, 1))
  bulkRead("composite small", compositeByteString(100, 1))

  singleRead("composite large", compositeByteString(100, 1024 * 10))
  bulkRead("composite large", compositeByteString(100, 1024 * 10))

  singleRead("empty", ByteString.empty)
  bulkRead("empty", ByteString.empty)
}
