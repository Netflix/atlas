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
  * Mutable double to integer map based on an underlying LongIntHashMap.
  * Primary use-case is computing a count for the number of times a particular
  * value was encountered.
  *
  * @param noData
  *     Value to use to represent no data in the array. This value should not
  *     be used in the input. Default is NaN.
  * @param capacity
  *     Initial capacity guideline. The actual size of the underlying buffer
  *     will be the next prime >= `capacity`. Default is 10.
  */
class DoubleIntHashMap(noData: Double = Double.NaN, capacity: Int = 10) {

  private[this] val data = new LongIntHashMap(d2l(noData), capacity)

  private def d2l(k: Double): Long = java.lang.Double.doubleToLongBits(k)

  private def l2d(k: Long): Double = java.lang.Double.longBitsToDouble(k)

  /**
    * Put a double to integer pair into the map. The key, `k`, should not be
    * equivalent to the `noData` value used for this map. If an entry with the
    * same key already exists, then the value will be overwritten.
    */
  def put(k: Double, v: Int): Unit = data.put(d2l(k), v)

  /**
    * Get the value associated with key, `k`. If no value is present, then the
    * `dflt` value will be returned.
    */
  def get(k: Double, dflt: Int): Int = data.get(d2l(k), dflt)

  /**
    * Add one to the count associated with `k`. If the key is not already in the
    * map a new entry will be created with a count of 1.
    */
  def increment(k: Double): Unit = increment(k, 1)

  /**
    * Add `amount` to the count associated with `k`. If the key is not already in the
    * map a new entry will be created with a count of `amount`.
    */
  def increment(k: Double, amount: Int): Unit = data.increment(d2l(k), amount)

  /** Execute `f` for each item in the set. */
  def foreach(f: (Double, Int) => Unit): Unit = {
    data.foreach { (k, v) =>
      f(l2d(k), v)
    }
  }

  /** Return the number of items in the set. This is a constant time operation. */
  def size: Int = data.size

  /** Converts this set to a Map[Double, Int]. Used mostly for debugging and tests. */
  def toMap: Map[Double, Int] = {
    val builder = Map.newBuilder[Double, Int]
    foreach { (k, v) =>
      builder += k -> v
    }
    builder.result()
  }
}
