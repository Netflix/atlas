/*
 * Copyright 2014-2022 Netflix, Inc.
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

/**
  * Mutable integer set based on open-addressing. Primary use-case is deduping
  * integers so it only supports `add` and `foreach`.
  *
  * @param noData
  *     Value to use to represent no data in the array. This value should not
  *     be used in the input.
  * @param capacity
  *     Initial capacity guideline. The actual size of the underlying buffer
  *     will be the next prime >= `capacity`. Default is 10.
  */
class IntHashSet(noData: Int, capacity: Int = 10) {

  private[this] var data = newArray(capacity)
  private[this] var used = 0
  private[this] var cutoff = computeCutoff(data.length)

  // Set at 50% capacity to get reasonable tradeoff between performance and
  // memory use. See IntIntMap benchmark.
  private def computeCutoff(n: Int): Int = math.max(3, n / 2)

  private def hash(k: Int, length: Int): Int = {
    Hash.absOrZero(Hash.lowbias32(k)) % length
  }

  private def newArray(n: Int): Array[Int] = {
    val tmp = new Array[Int](PrimeFinder.nextPrime(n))
    var i = 0
    while (i < tmp.length) {
      tmp(i) = noData
      i += 1
    }
    tmp
  }

  private def resize(): Unit = {
    val tmp = newArray(data.length * 2)
    var i = 0
    while (i < data.length) {
      val v = data(i)
      if (v != noData) add(tmp, v)
      i += 1
    }
    data = tmp
    cutoff = computeCutoff(data.length)
  }

  private def add(buffer: Array[Int], v: Int): Boolean = {
    var pos = hash(v, buffer.length)
    var posV = buffer(pos)
    while (posV != noData && posV != v) {
      pos = (pos + 1) % buffer.length
      posV = buffer(pos)
    }
    buffer(pos) = v
    posV == noData
  }

  /**
    * Add an integer into the set. The value, `v`, should not be equivalent to the
    * `noData` value used for this set.
    */
  def add(v: Int): Unit = {
    if (used >= cutoff) resize()
    if (add(data, v)) used += 1
  }

  /** Execute `f` for each item in the set. */
  def foreach(f: Int => Unit): Unit = {
    var i = 0
    while (i < data.length) {
      val v = data(i)
      if (v != noData) f(v)
      i += 1
    }
  }

  /** Return the number of items in the set. This is a constant time operation. */
  def size: Int = used

  /** Converts this set to an Array[Int]. */
  def toArray: Array[Int] = {
    val tmp = new Array[Int](used)
    var i = 0
    foreach { v =>
      tmp(i) = v
      i += 1
    }
    tmp
  }

  /** Converts this set to a List[Int]. Used mostly for debugging and tests. */
  def toList: List[Int] = {
    val builder = List.newBuilder[Int]
    foreach { v =>
      builder += v
    }
    builder.result()
  }
}
