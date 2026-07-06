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
package com.netflix.atlas.core.util

object RefIntHashMap {

  /**
    * Consumer for [[RefIntHashMap.foreach]]. A single-abstract-method trait with a
    * primitive `int` value parameter, so iteration does not box the value the way a
    * `Function2[T, Int, Unit]` would. Call sites can still pass a lambda; it is
    * converted to this SAM type.
    */
  trait Consumer[T] {
    def accept(key: T, value: Int): Unit
  }
}

/**
  * Mutable reference to integer map based on open-addressing. Primary use-case is
  * computing a count for the number of times a particular value was encountered.
  *
  * @param capacity
  *     Initial capacity guideline. The actual size of the underlying buffer
  *     will be the next prime >= `capacity`. Default is 10.
  */
class RefIntHashMap[T <: AnyRef](capacity: Int = 10) {

  private[this] var keys = newArray(capacity)
  private[this] var values = new Array[Int](keys.length)
  private[this] var used = 0
  private[this] var cutoff = computeCutoff(keys.length)

  // Set at 50% capacity to get reasonable tradeoff between performance and
  // memory use. See IntIntMap benchmark.
  private def computeCutoff(n: Int): Int = math.max(3, n / 2)

  private def newArray(n: Int): Array[T] = {
    ArrayHelper.newInstance[T](PrimeFinder.nextPrime(n))
  }

  // The raw `hashCode` has weak high-bit dispersion, so mix it with `lowbias32`
  // before reducing into the slot range (the reduction keys off the high bits).
  private def hash(k: T, length: Int): Int = {
    Hash.reduce(Hash.lowbias32(k.hashCode()), length)
  }

  private def resize(): Unit = {
    val tmpKS = newArray(keys.length * 2)
    val tmpVS = new Array[Int](tmpKS.length)
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

  private def put(ks: Array[T], vs: Array[Int], k: T, v: Int): Boolean = {
    var pos = hash(k, ks.length)
    var posV = ks(pos)
    while (posV != null && posV != k) {
      pos = if (pos + 1 < ks.length) pos + 1 else 0
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
  def put(k: T, v: Int): Unit = {
    if (used >= cutoff) resize()
    if (put(keys, values, k, v)) used += 1
  }

  /**
    * Put ref to integer pair into the map if there is not already a mapping
    * for `k`. Returns true if the value was inserted into the map.
    */
  def putIfAbsent(k: T, v: Int): Boolean = {
    if (used >= cutoff) resize()
    var pos = hash(k, keys.length)
    var posV = keys(pos)
    while (posV != null && posV != k) {
      pos = if (pos + 1 < keys.length) pos + 1 else 0
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
  def get(k: T, dflt: Int): Int = {
    val start = hash(k, keys.length)
    var pos = start
    while (true) {
      val prev = keys(pos)
      if (prev == null)
        return dflt
      else if (prev.equals(k))
        return values(pos)
      else {
        pos = if (pos + 1 < keys.length) pos + 1 else 0
        // If we've wrapped around to the start, the key is not present
        if (pos == start)
          return dflt
      }
    }
    dflt
  }

  /**
    * Add one to the count associated with `k`. If the key is not already in the
    * map a new entry will be created with a count of 1.
    */
  def increment(k: T): Unit = increment(k, 1)

  /**
    * Add `amount` to the count associated with `k`. If the key is not already in the
    * map a new entry will be created with a count of `amount`.
    */
  def increment(k: T, amount: Int): Unit = {
    if (used >= cutoff) resize()
    var pos = hash(k, keys.length)
    while (true) {
      val prev = keys(pos)
      if (prev == null || prev == k) {
        keys(pos) = k
        values(pos) += amount
        if (prev == null) used += 1
        return
      }
      pos = if (pos + 1 < keys.length) pos + 1 else 0
    }
  }

  /**
    * Execute `f` for each item in the set. Uses a specialized consumer rather than
    * a `Function2[T, Int, Unit]` so the int value is not boxed on each call (a
    * `Function2` with a reference-typed parameter falls back to the generic
    * `apply(Object, Object)`, which boxes the int).
    */
  def foreach(f: RefIntHashMap.Consumer[T]): Unit = {
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      if (k != null) f.accept(k, values(i))
      i += 1
    }
  }

  /** Return the number of items in the set. This is a constant time operation. */
  def size: Int = used

  /** Apply a mapping function `f` and converts the result to an array. */
  def mapToArray[R](buffer: Array[R])(f: (T, Int) => R): Array[R] = {
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
  def toMap: Map[T, Int] = {
    val builder = Map.newBuilder[T, Int]
    foreach { (k, v) =>
      builder += k -> v
    }
    builder.result()
  }
}
