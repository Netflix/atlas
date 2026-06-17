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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Strings
import munit.FunSuite

import java.math.BigInteger

class ItemIdSuite extends FunSuite {

  def testByteArray: Array[Byte] = {
    (1 to 20).map(_.toByte).toArray
  }

  test("create from byte array") {
    val bytes = testByteArray
    val id = ItemId(bytes)
    assertEquals(id.hashCode(), 219025168)
  }

  test("equals") {
    val id1 = ItemId(testByteArray)
    val id2 = ItemId(testByteArray)
    assertEquals(id1, id1)
    assertEquals(id1, id2)
  }

  test("not equals") {
    val id1 = ItemId(testByteArray)
    val bytes = testByteArray
    bytes(13) = 3.toByte // perturb byte used with hashing
    val id2 = ItemId(bytes)
    assertNotEquals(id1, id2)
    assertNotEquals(id1.hashCode(), id2.hashCode())
  }

  test("does not equal wrong object type") {
    val id1 = ItemId(testByteArray)
    assert(!id1.equals("foo"))
  }

  test("does not equal null") {
    val id1 = ItemId(testByteArray)
    assert(id1 != null)
  }

  test("toString") {
    val bytes = testByteArray
    val id = ItemId(bytes)
    assertEquals(id.toString, "0102030405060708090a0b0c0d0e0f1011121314")
  }

  test("toString sha1 bytes 0") {
    val bytes = new Array[Byte](20)
    val id = ItemId(bytes)
    assertEquals(id.toString, "0000000000000000000000000000000000000000")
    assertEquals(id, ItemId("0000000000000000000000000000000000000000"))
  }

  test("toString sha1") {
    (0 until 100).foreach { i =>
      val sha1 = Hash.sha1(i.toString)
      val sha1str = Strings.zeroPad(sha1.toString(16), 40)
      val id = ItemId(Hash.sha1bytes(i.toString))
      assertEquals(id.toString, sha1str)
      assertEquals(id, ItemId(sha1str))
      assertEquals(id.compareTo(ItemId(sha1str)), 0)
      assertEquals(sha1, ItemId(sha1str).toBigInteger)
    }
  }

  test("from String lower") {
    val bytes = testByteArray
    val id = ItemId(bytes)
    assertEquals(id, ItemId("0102030405060708090a0b0c0d0e0f1011121314"))
  }

  test("from String upper") {
    val bytes = testByteArray
    val id = ItemId(bytes)
    assertEquals(id, ItemId("0102030405060708090A0B0C0D0E0F1011121314"))
  }

  test("from String short") {
    val id = ItemId(Strings.zeroPad("abc", 32))
    assertEquals(id, ItemId("abc"))
  }

  test("from BigInteger short") {
    val id = ItemId(new BigInteger("abc", 16))
    assertEquals(id, ItemId("abc"))
  }

  test("from String invalid") {
    intercept[IllegalArgumentException] {
      ItemId("0G")
    }
  }

  test("compareTo equals") {
    val id1 = ItemId(testByteArray)
    val id2 = ItemId(testByteArray)
    assertEquals(id1.compareTo(id1), 0)
    assertEquals(id1.compareTo(id2), 0)
  }

  test("compareTo less than/greater than") {
    val id1 = ItemId(testByteArray)
    val bytes = testByteArray
    bytes(bytes.length - 1) = 21.toByte
    val id2 = ItemId(bytes)
    assert(id1.compareTo(id2) < 0)
    assert(id2.compareTo(id1) > 0)
  }

  test("compareTo agrees with unsigned byte comparison incl. prefix collisions") {
    def bytes(init: Int => Int): Array[Byte] = Array.tabulate(20)(i => init(i).toByte)
    val base = bytes(i => i + 1)
    // Differ only after the first 8 bytes -> exercises the fast-path fallback.
    val late = base.clone(); late(15) = (late(15) + 1).toByte
    // Differ within the first 8 bytes -> resolved by the fast path.
    val early = base.clone(); early(3) = (early(3) + 1).toByte
    // High bit set in byte 0 -> must compare as unsigned, not signed.
    val high = base.clone(); high(0) = 0x80.toByte

    val a = ItemId(base)
    assertEquals(a.compareTo(ItemId(base.clone())), 0)
    assert(a.compareTo(ItemId(late)) < 0)
    assert(ItemId(late).compareTo(a) > 0)
    assert(a.compareTo(ItemId(early)) < 0)
    assert(a.compareTo(ItemId(high)) < 0) // unsigned: 0x01.. < 0x80..

    // Variable-length ids (16 vs 20 bytes) that share an 8-byte prefix: the fast path ties and
    // the fallback must order the shorter (a prefix of the longer) first.
    val id16 = ItemId(Array.tabulate(16)(i => (i + 1).toByte))
    val id20 = ItemId(Array.tabulate(20)(i => (i + 1).toByte))
    assert(id16.compareTo(id20) < 0)
    assert(id20.compareTo(id16) > 0)

    // Cross-check the sign against java.util.Arrays.compareUnsigned over random pairs that
    // sometimes share an 8-byte prefix.
    val rnd = new scala.util.Random(1)
    (0 until 5000).foreach { _ =>
      val x = bytes(_ => rnd.nextInt(256))
      val y = x.clone()
      if (rnd.nextBoolean())
        y(8 + rnd.nextInt(12)) = rnd.nextInt(256).toByte // shared 8-byte prefix
      else y(rnd.nextInt(20)) = rnd.nextInt(256).toByte
      val expected = math.signum(java.util.Arrays.compareUnsigned(x, y))
      assertEquals(math.signum(ItemId(x).compareTo(ItemId(y))), expected)
    }
  }

  test("int value") {
    (0 until 100).foreach { i =>
      val id = ItemId(Hash.sha1bytes(i.toString))
      assertEquals(id.intValue, id.toBigInteger.intValue())
    }
  }
}
