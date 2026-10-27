/*
 * Copyright 2014-2026 Netflix, Inc.
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

import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.util.Random
import org.apache.pekko.util.ByteString
import munit.FunSuite

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import scala.util.Using

class ByteStringInputStreamSuite extends FunSuite {

  private def compress(data: ByteString): ByteString = {
    val baos = new ByteArrayOutputStream()
    Using.resource(new GZIPOutputStream(baos)) { out =>
      out.write(data.toArray)
    }
    ByteString(baos.toByteArray)
  }

  private def singleRead(name: String, data: ByteString): Unit = {
    test(s"$name: read() and available()") {
      val bais = new ByteArrayInputStream(data.toArray)
      val bsis = ByteStringInputStream.create(data)
      val gzis = ByteStringInputStream.create(compress(data))

      data.indices.foreach { i =>
        if (data.isCompact) {
          assertEquals(bais.available(), data.length - i)
          assertEquals(bsis.available(), data.length - i)
        } else {
          assert(bsis.available() > 0)
        }
        assert(gzis.available() > 0)

        val expected = bais.read()
        assertEquals(expected, bsis.read())
        assertEquals(expected, gzis.read())
      }

      val expected = bais.read()
      assertEquals(expected, bsis.read())
      assertEquals(expected, gzis.read())

      bsis.close()
      gzis.close()
    }
  }

  private def bulkRead(name: String, data: ByteString): Unit = {
    test(s"$name: read(buffer, offset, length)") {
      val bais = new ByteArrayInputStream(data.toArray)
      val bsis = ByteStringInputStream.create(data)
      val gzis = ByteStringInputStream.create(compress(data))

      val h1 = MessageDigest.getInstance("SHA-256")
      val h2 = MessageDigest.getInstance("SHA-256")
      val h3 = MessageDigest.getInstance("SHA-256")

      val b1 = new Array[Byte](13)
      val b2 = new Array[Byte](13)
      val b3 = new Array[Byte](13)
      var continue = true
      while (continue) {
        val len1 = bais.read(b1)
        val len2 = bsis.read(b2)
        val len3 = gzis.read(b3)
        if (data.isCompact) {
          assertEquals(len1, len2)
          assertEquals(b1.toSeq, b2.toSeq)
        }
        if (len1 > 0) h1.update(b1, 0, len1)
        if (len2 > 0) h2.update(b2, 0, len2)
        if (len3 > 0) h3.update(b3, 0, len3)
        continue = len1 > 0 || len2 > 0 || len3 > 0
      }

      val digest = h1.digest().toSeq
      assertEquals(digest, h2.digest().toSeq)
      assertEquals(digest, h3.digest().toSeq)

      bsis.close()
      gzis.close()
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

  test("read with zero length returns 0 when data remains") {
    val in = new ByteStringInputStream(ByteString("abc"))
    assertEquals(in.read(new Array[Byte](4), 0, 0), 0)
    assertEquals(in.read(), 'a'.toInt)
  }

  test("read with zero length returns 0 at end of stream") {
    val in = new ByteStringInputStream(ByteString.empty)
    assertEquals(in.read(new Array[Byte](0)), 0)
    assertEquals(in.read(new Array[Byte](4)), -1)
  }

  test("read with invalid offset or length throws") {
    // Less data remaining than the requested length, so the bounds check must not depend
    // on how many bytes would actually be copied
    val in = new ByteStringInputStream(ByteString("a"))
    val buf = new Array[Byte](4)
    intercept[IndexOutOfBoundsException](in.read(buf, -1, 1))
    intercept[IndexOutOfBoundsException](in.read(buf, 0, -1))
    intercept[IndexOutOfBoundsException](in.read(buf, 2, 3))
    assertEquals(in.read(), 'a'.toInt)
  }

  test("read with invalid offset or length throws at end of stream") {
    val in = new ByteStringInputStream(ByteString.empty)
    intercept[IndexOutOfBoundsException](in.read(new Array[Byte](4), 10, 5))
  }
}
