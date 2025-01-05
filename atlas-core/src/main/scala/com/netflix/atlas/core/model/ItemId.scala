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
package com.netflix.atlas.core.model

import java.math.BigInteger
import com.netflix.atlas.core.util.Strings

import java.lang.invoke.MethodHandles
import java.nio.ByteOrder

/**
  * Represents an identifier for a tagged item. If using for a hash map the
  * bytes used for the id should come from a decent hash function as 4 bytes
  * from the middle are used for the hash code of the id object.
  *
  * @param data
  *     Bytes for the id. This is usually the results of computing a SHA1 hash
  *     over a normalized representation of the tags.
  */
class ItemId private (private val data: Array[Byte]) extends Comparable[ItemId] {

  // Typically it should be 20 bytes for SHA1. Require at least 16 to avoid
  // checks for other operations.
  require(data.length >= 16)

  override def hashCode(): Int = {
    // Choose middle byte. The id should be generated using decent hash
    // function so in theory any subset will do. In some cases data is
    // routed based on the prefix or a modulo of the intValue. Choosing a
    // byte toward the middle helps to mitigate that.
    ItemId.intHandle.get(data, 12)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: ItemId => java.util.Arrays.equals(data, other.data)
      case _             => false
    }
  }

  override def compareTo(other: ItemId): Int = {
    java.util.Arrays.compareUnsigned(data, other.data)
  }

  override def toString: String = {
    val buffer = new java.lang.StringBuilder(data.length * 2)
    var i = 0
    while (i < data.length) {
      val unsigned = java.lang.Byte.toUnsignedInt(data(i))
      buffer.append(ItemId.hexValueForByte(unsigned))
      i += 1
    }
    buffer.toString
  }

  def toBigInteger: BigInteger = new BigInteger(1, data)

  def intValue: Int = {
    ItemId.intHandle.get(data, data.length - 4)
  }
}

object ItemId {

  // Helper to access integer from byte array
  private val intHandle =
    MethodHandles.byteArrayViewVarHandle(classOf[Array[Int]], ByteOrder.BIG_ENDIAN)

  private val hexValueForByte = (0 until 256).toArray.map { i =>
    Strings.zeroPad(i, 2)
  }

  /**
    * Create a new id from an array of bytes.
    */
  def apply(data: Array[Byte]): ItemId = {
    new ItemId(data)
  }

  /**
    * Create a new id from a BigInteger instance.
    */
  def apply(data: BigInteger): ItemId = {
    apply(data.toString(16))
  }

  /**
    * Create a new id from a hex string. The string should match the `toString` output of
    * an `ItemId`.
    */
  def apply(data: String): ItemId = {
    // Pad to min size for id. Allows it to work easily with hex strings from number types
    val str = Strings.zeroPad(data, 32)
    require(str.length % 2 == 0, s"invalid item id string: $str")
    val bytes = new Array[Byte](str.length / 2)
    var i = 0
    while (i < bytes.length) {
      val c1 = hexToInt(str.charAt(2 * i))
      val c2 = hexToInt(str.charAt(2 * i + 1))
      val v = (c1 << 4) | c2
      bytes(i) = v.toByte
      i += 1
    }
    new ItemId(bytes)
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
