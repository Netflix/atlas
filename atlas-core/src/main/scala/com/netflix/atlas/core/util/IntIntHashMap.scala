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

/**
  * Mutable integer map based on open-addressing. Primary use-case is computing
  * a count for the number of times a particular value was encountered.
  *
  * @param noData
  *     Value to use to represent no data in the array. This value should not
  *     be used in the input.
  * @param capacity
  *     Initial capacity guideline. The actual size of the underlying buffer
  *     will be the next prime >= `capacity`. Default is 10.
  */
class IntIntHashMap(noData: Int, capacity: Int = 10) {

  private[this] var keys = newArray(capacity)
  private[this] var values = new Array[Int](keys.length)
  private[this] var used = 0
  private[this] var cutoff = computeCutoff(keys.length)

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
    val tmpKS = newArray(keys.length * 2)
    val tmpVS = new Array[Int](tmpKS.length)
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      if (k != noData) put(tmpKS, tmpVS, k, values(i))
      i += 1
    }
    keys = tmpKS
    values = tmpVS
    cutoff = computeCutoff(tmpKS.length)
  }

  private def put(ks: Array[Int], vs: Array[Int], k: Int, v: Int): Boolean = {
    var pos = hash(k, ks.length)
    var posV = ks(pos)
    while (posV != noData && posV != k) {
      pos = (pos + 1) % ks.length
      posV = ks(pos)
    }
    ks(pos) = k
    vs(pos) = v
    posV == noData
  }

  /**
    * Put an integer key/value pair into the map. The key, `k`, should not be
    * equivalent to the `noData` value used for this map. If an entry with the
    * same key already exists, then the value will be overwritten.
    */
  def put(k: Int, v: Int): Unit = {
    if (used >= cutoff) resize()
    if (put(keys, values, k, v)) used += 1
  }

  /**
    * Get the value associated with key, `k`. If no value is present, then the
    * `dflt` value will be returned.
    */
  def get(k: Int, dflt: Int): Int = {
    var pos = hash(k, keys.length)
    while (true) {
      val prev = keys(pos)
      if (prev == noData)
        return dflt
      else if (prev == k)
        return values(pos)
      else
        pos = (pos + 1) % keys.length
    }
    dflt
  }

  /**
    * Add one to the count associated with `k`. If the key is not already in the
    * map a new entry will be created with a count of 1.
    */
  def increment(k: Int): Unit = increment(k, 1)

  /**
    * Add `amount` to the count associated with `k`. If the key is not already in the
    * map a new entry will be created with a count of `amount`.
    */
  def increment(k: Int, amount: Int): Unit = {
    if (used >= cutoff) resize()
    var pos = hash(k, keys.length)
    var prev = keys(pos)
    while (prev != noData && prev != k) {
      pos = (pos + 1) % keys.length
      prev = keys(pos)
    }
    keys(pos) = k
    values(pos) += amount
    if (prev == noData) used += 1
  }

  /** Execute `f` for each item in the set. */
  def foreach(f: (Int, Int) => Unit): Unit = {
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      if (k != noData) f(k, values(i))
      i += 1
    }
  }

  /** Return the number of items in the set. This is a constant time operation. */
  def size: Int = used

  /** Converts this set to a Map[Int, Int]. Used mostly for debugging and tests. */
  def toMap: Map[Int, Int] = {
    val builder = Map.newBuilder[Int, Int]
    foreach { (k, v) =>
      builder += k -> v
    }
    builder.result()
  }
}
