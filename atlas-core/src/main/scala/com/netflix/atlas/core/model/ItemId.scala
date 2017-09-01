/*
 * Copyright 2014-2017 Netflix, Inc.
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

import java.math.BigInteger

import com.netflix.atlas.core.util.Strings

import scala.util.hashing.MurmurHash3

/**
  * Represents an identifier for a tagged item.
  *
  * @param data
  *     Bytes for the id. This is usually the results of computing a SHA1 hash
  *     over a normalized representation of the tags.
  * @param hc
  *     Precomputed hash code for the bytes.
  */
class ItemId private (private val data: Array[Byte], private val hc: Int)
    extends Comparable[ItemId] {

  override def hashCode(): Int = hc

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: ItemId => hc == other.hc && java.util.Arrays.equals(data, other.data)
      case _             => false
    }
  }

  override def compareTo(other: ItemId): Int = {
    val length = math.min(data.length, other.data.length)
    var i = 0
    while (i < length) {
      val b1 = java.lang.Byte.toUnsignedInt(data(i))
      val b2 = java.lang.Byte.toUnsignedInt(other.data(i))
      val cmp = b1 - b2
      if (cmp != 0) return cmp
      i += 1
    }
    0
  }

  override def toString: String = {
    val buffer = new StringBuilder
    var i = 0
    while (i < data.length) {
      val unsigned = java.lang.Byte.toUnsignedInt(data(i))
      buffer.append(ItemId.hexValueForByte(unsigned))
      i += 1
    }
    buffer.toString()
  }

  def toBigInteger: BigInteger = new BigInteger(1, data)
}

object ItemId {
  private val hexValueForByte = (0 until 256).toArray.map { i =>
    Strings.zeroPad(i, 2)
  }

  /**
    * Create a new id from an array of bytes. The pre-computed hash code will be generated
    * using MurmurHash3.
    */
  def apply(data: Array[Byte]): ItemId = {
    new ItemId(data, MurmurHash3.bytesHash(data))
  }

  /**
    * Create a new id from a hex string. The string should match the `toString` output of
    * an `ItemId`.
    */
  def apply(data: String): ItemId = {
    require(data.length % 2 == 0, s"invalid item id string: $data")
    val bytes = new Array[Byte](data.length / 2)
    var i = 0
    while (i < bytes.length) {
      val c1 = hexToInt(data.charAt(2 * i))
      val c2 = hexToInt(data.charAt(2 * i + 1))
      val v = (c1 << 4) | c2
      bytes(i) = v.toByte
      i += 1
    }
    ItemId(bytes)
  }

  private def hexToInt(c: Char): Int = {
    c match {
      case _ if c >= '0' && c <= '9' => c - '0'
      case _ if c >= 'a' && c <= 'f' => c - 'a' + 10
      case _ if c >= 'A' && c <= 'F' => c - 'A' + 10
      case _                         => throw new IllegalArgumentException(s"invalid hex digit: $c")
    }
  }
}
