/*
 * Copyright 2015 Netflix, Inc.
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

object SmallHashMap {
  def empty[K <: AnyRef, V <: AnyRef]: SmallHashMap[K, V] = new SmallHashMap[K, V](Array.empty, 0)

  def apply[K <: AnyRef, V <: AnyRef](ts: (K, V)*): SmallHashMap[K, V] = {
    apply(ts.size, ts.iterator)
  }

  def apply[K <: AnyRef, V <: AnyRef](ts: Iterable[(K, V)]): SmallHashMap[K, V] = {
    apply(ts.toSeq)
  }

  def apply[K <: AnyRef, V <: AnyRef](length: Int, iter: Iterator[(K, V)]): SmallHashMap[K, V] = {
    val b = new Builder[K, V](length)
    while (iter.hasNext) {
      val t = iter.next()
      b.add(t._1, t._2)
    }
    b.result
  }

  class Builder[K <: AnyRef, V <: AnyRef](size: Int) {
    private val buf = new Array[AnyRef](size * 2)
    private var actualSize = 0

    def +=(pair: (K, V)): Unit = add(pair._1, pair._2)

    def add(k: K, v: V) {
      val pos = math.abs(k.hashCode) % size
      var i = pos
      var ki = buf(i * 2)
      var keq = (ki == k)
      while (ki != null && !keq) {
        i = (i + 1) % size
        require(i != pos, "data array is full")
        ki = buf(i * 2)
        keq = (ki == k)
      }

      if (keq) {
        buf(i * 2) = k
        buf(i * 2 + 1) = v
      } else {
        require(buf(i * 2) == null, "position has already been filled")
        buf(i * 2) = k
        buf(i * 2 + 1) = v
        actualSize += 1
      }
    }

    def addAll(m: Map[K, V]) {
      m match {
        case sm: SmallHashMap[K, V] =>
          var i = 0
          while (i < sm.data.length) {
            val k = sm.data(i).asInstanceOf[K]
            if (k != null) add(k, sm.data(i + 1).asInstanceOf[V])
            i += 2
          }
        case _ => m.foreach { t => add(t._1, t._2) }
      }
    }

    def result: SmallHashMap[K, V] = {
      new SmallHashMap[K, V](buf, actualSize)
    }

    def compact: SmallHashMap[K, V] = {
      if (actualSize == size) {
        new SmallHashMap[K, V](buf, actualSize)
      } else {
        val b = new Builder[K, V](actualSize)
        var i = 0
        while (i < buf.length) {
          if (buf(i) != null) b.add(buf(i).asInstanceOf[K], buf(i + 1).asInstanceOf[V])
          i += 2
        }
        b.result
      }
    }
  }
}

/**
 * Simple immutable hash map implementation intended for use-cases where the number of entries is
 * known to be small. This implementation is backed by a single array and uses open addressing with
 * linear probing to resolve conflicts. The underlying array is created to exactly fit the data
 * size so hash collisions tend to be around 50%, but have a fairly low number of probes to find
 * the actual entry. With a cheap equals function for the keys lookups should be fast and there
 * is low memory overhead.
 *
 * You probably don't want to use this implementation if you expect more than around 50 keys in the
 * map. If you have millions of small immutable maps, such as tag data associated with metrics,
 * it may be a good fit.
 *
 * @param data        array with the items
 * @param dataLength  number of pairs contained within the array starting at index 0. Everything
 *                    in the array after 2 * dataLength will be ignored.
 */
class SmallHashMap[K <: AnyRef, V <: AnyRef](val data: Array[AnyRef], dataLength: Int)
    extends scala.collection.immutable.Map[K, V] {

  private[this] var cachedHashCode: Int = 0

  private def hash(k: AnyRef): Int = {
    val capacity = data.length / 2
    math.abs(k.hashCode) % capacity
  }

  def get(key: K): Option[V] = {
    val capacity = data.length / 2
    val pos = hash(key)
    var i = pos
    if (key != data(i * 2)) {
      i = (i + 1) % capacity
      while (key != data(i * 2) && i != pos) {
        i = (i + 1) % capacity
      }
    }
    val v = if (key == data(i * 2)) data(i * 2 + 1) else null
    Option(v.asInstanceOf[V])
  }

  override def foreach[U](f: ((K, V)) => U) {
    var i = 0
    while (i < data.length) {
      if (data(i) != null) f(data(i).asInstanceOf[K] -> data(i + 1).asInstanceOf[V])
      i += 2
    }
  }

  /**
   * Call the function `f` for each tuple in the map without requiring a temporary object to be
   * created.
   */
  def foreachItem(f: (K, V) => Unit) {
    var i = 0
    while (i < data.length) {
      if (data(i) != null) f(data(i).asInstanceOf[K], data(i + 1).asInstanceOf[V])
      i += 2
    }
  }

  def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    var i = 0
    var pos = 0
    def hasNext: Boolean = i < dataLength
    def next(): (K, V) = {
      while (data(pos) == null) {
        pos += 2
      }
      val t = data(pos).asInstanceOf[K] -> data(pos + 1).asInstanceOf[V]
      pos += 2
      i += 1
      t
    }
  }

  override def keysIterator: Iterator[K] = new Iterator[K] {
    var i = 0
    var pos = 0
    def hasNext: Boolean = i < dataLength
    def next(): K = {
      while (data(pos) == null) {
        pos += 2
      }
      val t = data(pos).asInstanceOf[K]
      pos += 2
      i += 1
      t
    }
  }

  override def valuesIterator: Iterator[V] = new Iterator[V] {
    var i = 0
    var pos = 0
    def hasNext: Boolean = i < dataLength
    def next(): V = {
      while (data(pos) == null) {
        pos += 2
      }
      val t = data(pos + 1).asInstanceOf[V]
      pos += 2
      i += 1
      t
    }
  }

  /**
   * Returns the number of keys that are not in the correct position based on their hash code.
   */
  def numCollisions: Int = {
    var count = 0
    var i = 0
    while (i < data.length) {
      if (data(i) != null && hash(data(i)) != i / 2) count += 1
      i += 2
    }
    count
  }

  /**
   * Returns the average number of probes that are required for looking up keys in this map. In
   * general we want this number to be less than N/4. If we naively did a linear scan of the
   * full data it would be N/2.
   */
  def numProbesPerKey: Double =  {
    var total = 0
    keys.foreach { k =>
      var i = hash(k)
      while (data(i * 2) != k) {
        total += 1
        i = (i + 1) % dataLength
      }
    }
    total.toDouble / dataLength
  }

  def +[B1 >: V](kv: (K, B1)): collection.immutable.Map[K, B1] = {
    Map(toSeq: _*) + kv
  }

  def -(k: K): collection.immutable.Map[K, V] = {
    Map(toSeq: _*) - k
  }

  def ++(m: Map[K, V]): collection.immutable.Map[K, V] = {
    val b = new SmallHashMap.Builder[K, V](size + m.size)
    b.addAll(this)
    b.addAll(m)
    b.result
  }

  override def size: Int = dataLength

  override def hashCode: Int = {
    // Pattern copied from String.java of jdk
    var h = cachedHashCode
    if (h == 0) {
      h = super.hashCode
      cachedHashCode = h
    }
    h
  }
}
