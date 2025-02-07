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
package com.netflix.atlas.core.util

import java.math.BigInteger
import java.security.MessageDigest

import scala.util.Try

object Hash {

  /** Hash function for use with 64-bit integers. */
  def lowbias64(v: Long): Int = {
    val h1 = lowbias32((v >>> 32).toInt)
    val h2 = lowbias32(v.toInt)
    h1 ^ h2
  }

  /**
    * Hash function for use with 32-bit integers. For more details on this hash see:
    * https://nullprogram.com/blog/2018/07/31/
    */
  def lowbias32(v: Int): Int = {
    var h = v
    h ^= h >>> 16
    h *= 0x7FEB352D
    h ^= h >>> 15
    h *= 0x846CA68B
    h ^= h >>> 16
    h
  }

  // Seeing contention on MessageDigest.getInstance, following pattern used by jruby:
  // https://github.com/jruby/jruby/commit/e840823c435393e8365be1bae93f646c1bb0043f
  private val cloneableDigests = createDigests()

  private def createDigests(): scala.collection.mutable.HashMap[String, MessageDigest] = {
    val digests = new scala.collection.mutable.HashMap[String, MessageDigest]()
    List("MD5", "SHA1").foreach { algorithm =>
      Try(MessageDigest.getInstance(algorithm)).foreach { digest =>
        // Try to clone the digest to make sure it is cloneable
        Try(digest.clone().asInstanceOf[MessageDigest]).foreach { clone =>
          digests += algorithm -> clone
        }
      }
    }
    digests
  }

  def get(algorithm: String): MessageDigest = {
    cloneableDigests.get(algorithm).fold(MessageDigest.getInstance(algorithm)) { digest =>
      digest.clone().asInstanceOf[MessageDigest]
    }
  }

  def md5(input: Array[Byte]): BigInteger = {
    computeHash("MD5", input)
  }

  def md5(input: String): BigInteger = {
    computeHash("MD5", input.getBytes("UTF-8"))
  }

  def sha1(input: Array[Byte]): BigInteger = {
    computeHash("SHA1", input)
  }

  def sha1(input: String): BigInteger = {
    computeHash("SHA1", input.getBytes("UTF-8"))
  }

  def sha1bytes(input: String): Array[Byte] = {
    computeHashBytes("SHA1", input.getBytes("UTF-8"))
  }

  // If the hash value is `Integer.MIN_VALUE`, then the absolute value will be
  // negative. For our purposes that will get mapped to a starting position of 0.
  private[util] def absOrZero(v: Int): Int = math.max(math.abs(v), 0)

  private def computeHash(algorithm: String, bytes: Array[Byte]): BigInteger = {
    new BigInteger(1, computeHashBytes(algorithm, bytes))
  }

  private def computeHashBytes(algorithm: String, bytes: Array[Byte]): Array[Byte] = {
    val md = get(algorithm)
    md.update(bytes)
    md.digest
  }
}
