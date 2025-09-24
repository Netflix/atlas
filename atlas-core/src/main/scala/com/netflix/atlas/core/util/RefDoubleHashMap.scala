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
  * Mutable reference to double map based on open-addressing. Primary use-case is
  * computing an aggregate double value based on a key.
  *
  * @param capacity
  *     Initial capacity guideline. The actual size of the underlying buffer
  *     will be the next prime >= `capacity`. Default is 10.
  */
class RefDoubleHashMap[T <: AnyRef](capacity: Int = 10) {

  private[this] var keys = newArray(capacity)
  private[this] var values = new Array[Double](keys.length)
  private[this] var used = 0
  private[this] var cutoff = computeCutoff(keys.length)

  // Set at 50% capacity to get reasonable tradeoff between performance and
  // memory use. See IntIntMap benchmark.
  private def computeCutoff(n: Int): Int = math.max(3, n / 2)

  private def newArray(n: Int): Array[T] = {
    ArrayHelper.newInstance[T](PrimeFinder.nextPrime(n))
  }

  private def resize(): Unit = {
    val tmpKS = newArray(keys.length * 2)
    val tmpVS = new Array[Double](tmpKS.length)
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      if (k != null) put(tmpKS, tmpVS, k, values(i))
      i += 1
    }
    keys = tmpKS
    values = tmpVS
    cutoff = computeCutoff(tmpKS.length)
  }

  private def put(ks: Array[T], vs: Array[Double], k: T, v: Double): Boolean = {
    var pos = Hash.absOrZero(k.hashCode()) % ks.length
    var posV = ks(pos)
    while (posV != null && posV != k) {
      pos = (pos + 1) % ks.length
      posV = ks(pos)
    }
    ks(pos) = k
    vs(pos) = v
    posV == null
  }

  /**
    * Put a ref to integer pair into the map. The key, `k`, should not be
    * equivalent to the `noData` value used for this map. If an entry with the
    * same key already exists, then the value will be overwritten.
    */
  def put(k: T, v: Double): Unit = {
    if (used >= cutoff) resize()
    if (put(keys, values, k, v)) used += 1
  }

  /**
    * Put ref to integer pair into the map if there is not already a mapping
    * for `k`. Returns true if the value was inserted into the map.
    */
  def putIfAbsent(k: T, v: Double): Boolean = {
    if (used >= cutoff) resize()
    var pos = Hash.absOrZero(k.hashCode()) % keys.length
    var posV = keys(pos)
    while (posV != null && posV != k) {
      pos = (pos + 1) % keys.length
      posV = keys(pos)
    }
    if (posV != null) false
    else {
      keys(pos) = k
      values(pos) = v
      used += 1
      true
    }
  }

  /**
    * Get the value associated with key, `k`. If no value is present, then the
    * `dflt` value will be returned.
    */
  def get(k: T, dflt: Double): Double = {
    var pos = Hash.absOrZero(k.hashCode()) % keys.length
    while (true) {
      val prev = keys(pos)
      if (prev == null)
        return dflt
      else if (prev.equals(k))
        return values(pos)
      else
        pos = (pos + 1) % keys.length
    }
    dflt
  }

  /**
    * Add `amount` to the value associated with `k`. If the key is not already in the
    * map a new entry will be created with a value of `amount`.
    */
  def add(k: T, amount: Double): Unit = {
    if (used >= cutoff) resize()
    var pos = Hash.absOrZero(k.hashCode()) % keys.length
    while (true) {
      val prev = keys(pos)
      if (prev == null || prev == k) {
        keys(pos) = k
        values(pos) += amount
        if (prev == null) used += 1
        return
      }
      pos = (pos + 1) % keys.length
    }
  }

  /**
    * Compute max `amount` and the value associated with `k`. If the key is not already in
    * the map a new entry will be created with a value of `amount`.
    */
  def max(k: T, amount: Double): Unit = {
    if (used >= cutoff) resize()
    var pos = Hash.absOrZero(k.hashCode()) % keys.length
    while (true) {
      val prev = keys(pos)
      if (prev == null || prev == k) {
        keys(pos) = k
        if (prev == null) {
          values(pos) = amount
          used += 1
        } else {
          values(pos) = math.max(values(pos), amount)
        }
        return
      }
      pos = (pos + 1) % keys.length
    }
  }

  /**
    * Compute min `amount` and the value associated with `k`. If the key is not already in
    * the map a new entry will be created with a value of `amount`.
    */
  def min(k: T, amount: Double): Unit = {
    if (used >= cutoff) resize()
    var pos = Hash.absOrZero(k.hashCode()) % keys.length
    while (true) {
      val prev = keys(pos)
      if (prev == null || prev == k) {
        keys(pos) = k
        if (prev == null) {
          values(pos) = amount
          used += 1
        } else {
          values(pos) = math.min(values(pos), amount)
        }
        return
      }
      pos = (pos + 1) % keys.length
    }
  }

  /** Execute `f` for each item in the set. */
  def foreach(f: (T, Double) => Unit): Unit = {
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      if (k != null) f(k, values(i))
      i += 1
    }
  }

  /** Return the number of items in the set. This is a constant time operation. */
  def size: Int = used

  /** Apply a mapping function `f` and converts the result to an array. */
  def mapToArray[R](buffer: Array[R])(f: (T, Double) => R): Array[R] = {
    require(buffer.length == used, s"buffer.length (${buffer.length}) != size ($used)")
    var i = 0
    var j = 0
    while (i < keys.length) {
      val k = keys(i)
      if (k != null) {
        buffer(j) = f(k, values(i))
        j += 1
      }
      i += 1
    }
    buffer
  }

  /** Converts this set to a Map[T, Int]. Used mostly for debugging and tests. */
  def toMap: Map[T, Double] = {
    val builder = Map.newBuilder[T, Double]
    foreach { (k, v) =>
      builder += k -> v
    }
    builder.result()
  }
}
