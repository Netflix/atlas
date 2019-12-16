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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Strings
import org.scalatest.funsuite.AnyFunSuite

import scala.util.hashing.MurmurHash3

class ItemIdSuite extends AnyFunSuite {

  test("create from byte array") {
    val bytes = Array(1.toByte, 2.toByte)
    val id = ItemId(bytes)
    assert(id.hashCode() === MurmurHash3.bytesHash(bytes))
  }

  test("equals") {
    val id1 = ItemId(Array(1.toByte, 2.toByte))
    val id2 = ItemId(Array(1.toByte, 2.toByte))
    assert(id1 === id1)
    assert(id1 === id2)
  }

  test("not equals") {
    val id1 = ItemId(Array(1.toByte, 2.toByte))
    val id2 = ItemId(Array(1.toByte, 3.toByte))
    assert(id1 !== id2)
    assert(id1.hashCode() !== id2.hashCode())
  }

  test("does not equal wrong object type") {
    val id1 = ItemId(Array(1.toByte, 2.toByte))
    assert(id1 !== "foo")
  }

  test("does not equal null") {
    val id1 = ItemId(Array(1.toByte, 2.toByte))
    assert(id1 !== null)
  }

  test("toString") {
    val bytes = Array(1.toByte, 2.toByte)
    val id = ItemId(bytes)
    assert(id.toString === "0102")
  }

  test("toString sha1 bytes 0") {
    val bytes = new Array[Byte](20)
    val id = ItemId(bytes)
    assert(id.toString === "0000000000000000000000000000000000000000")
    assert(id === ItemId("0000000000000000000000000000000000000000"))
  }

  test("toString sha1") {
    (0 until 100).foreach { i =>
      val sha1 = Hash.sha1(i.toString)
      val sha1str = Strings.zeroPad(sha1.toString(16), 40)
      val id = ItemId(Hash.sha1bytes(i.toString))
      assert(id.toString === sha1str)
      assert(id === ItemId(sha1str))
      assert(id.compareTo(ItemId(sha1str)) === 0)
      assert(sha1 === ItemId(sha1str).toBigInteger)
    }
  }

  test("from String lower") {
    val bytes = Array(10.toByte, 11.toByte)
    val id = ItemId(bytes)
    assert(id === ItemId("0a0b"))
  }

  test("from String upper") {
    val bytes = Array(10.toByte, 11.toByte)
    val id = ItemId(bytes)
    assert(id === ItemId("0A0B"))
  }

  test("from String invalid") {
    intercept[IllegalArgumentException] {
      ItemId("0G")
    }
  }

  test("compareTo equals") {
    val id1 = ItemId(Array(1.toByte, 2.toByte))
    val id2 = ItemId(Array(1.toByte, 2.toByte))
    assert(id1.compareTo(id1) === 0)
    assert(id1.compareTo(id2) === 0)
  }

  test("compareTo less than/greater than") {
    val id1 = ItemId(Array(1.toByte, 2.toByte))
    val id2 = ItemId(Array(1.toByte, 3.toByte))
    assert(id1.compareTo(id2) < 0)
    assert(id2.compareTo(id1) > 0)
  }
}
