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
package com.netflix.atlas.core.util

object SmallHashMap {
  def empty[K <: Any, V <: Any]: SmallHashMap[K, V] = new SmallHashMap[K, V](Array.empty, 0)

  def apply[K <: Any, V <: Any](ts: (K, V)*): SmallHashMap[K, V] = {
    apply(ts.size, ts.iterator)
  }

  def apply[K <: Any, V <: Any](ts: Iterable[(K, V)]): SmallHashMap[K, V] = {
    val seq = ts.toSeq
    apply(seq.size, seq.iterator)
  }

  def apply[K <: Any, V <: Any](length: Int, iter: Iterator[(K, V)]): SmallHashMap[K, V] = {
    val b = new Builder[K, V](length)
    while (iter.hasNext) {
      val t = iter.next()
      b.add(t._1, t._2)
    }
    b.result
  }

  class Builder[K <: Any, V <: Any](size: Int) {
    private val buf = new Array[Any](size * 2)
    private var actualSize = 0

    def +=(pair: (K, V)): Unit = add(pair._1, pair._2)

    def add(k: K, v: V) {
      val pos = Hash.absOrZero(k.hashCode) % size
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

  class EntryIterator[K <: Any, V <: Any](map: SmallHashMap[K, V]) extends Iterator[(K, V)] {
    private final val len = map.data.length
    var pos = 0
    skipEmptyEntries()

    def hasNext: Boolean = pos < len

    def next(): (K, V) = {
      val t = key -> value
      nextEntry()
      t
    }

    def nextEntry(): Unit = {
      pos += 2
      skipEmptyEntries()
    }

    private def skipEmptyEntries(): Unit = {
      while (pos < len && map.data(pos) == null) {
        pos += 2
      }
    }

    @inline def key: K = map.data(pos).asInstanceOf[K]

    @inline def value: V = map.data(pos + 1).asInstanceOf[V]
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
 * @param dataLength  number of pairs contained within the array starting at index 0.
 */
final class SmallHashMap[K <: Any, V <: Any] private (val data: Array[Any], dataLength: Int)
    extends scala.collection.immutable.Map[K, V] {

  require(data.length % 2 == 0)

  private[this] var cachedHashCode: Int = 0

  private def hash(k: Any): Int = {
    val capacity = data.length / 2
    Hash.absOrZero(k.hashCode) % capacity
  }

  def getOrNull(key: K): V = {
    if (dataLength == 0) return null.asInstanceOf[V]
    val capacity = data.length / 2
    val pos = hash(key)
    var i = pos
    var ki = data(i * 2)
    if (ki != null && ki != key) {
      do {
        i = (i + 1) % capacity
        ki = data(i * 2)
      } while (ki != null && ki != key && i != pos)
    }
    val v = if (ki != null && ki == key) data(i * 2 + 1) else null
    v.asInstanceOf[V]
  }

  def get(key: K): Option[V] = Option(getOrNull(key))

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

  def find(f: (K, V) => Boolean): Option[(K, V)] = {
    var i = 0
    while (i < data.length) {
      if (data(i) != null && f(data(i).asInstanceOf[K], data(i + 1).asInstanceOf[V])) {
        return Some(data(i).asInstanceOf[K] -> data(i + 1).asInstanceOf[V])
      }
      i += 2
    }
    None
  }

  def entriesIterator: SmallHashMap.EntryIterator[K, V] = {
    new SmallHashMap.EntryIterator[K, V](this)
  }

  def iterator: Iterator[(K, V)] = entriesIterator

  override def keysIterator: Iterator[K] = new Iterator[K] {
    val iter = entriesIterator
    def hasNext: Boolean = iter.hasNext
    def next(): K = {
      val k = iter.key
      iter.nextEntry()
      k
    }
  }

  override def valuesIterator: Iterator[V] = new Iterator[V] {
    val iter = entriesIterator
    def hasNext: Boolean = iter.hasNext
    def next(): V = {
      val v = iter.value
      iter.nextEntry()
      v
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

  def +[V1 >: V](kv: (K, V1)): collection.immutable.Map[K, V1] = {
    val b = new SmallHashMap.Builder[K, V1](size + 1)
    foreachItem(b.add)
    b.add(kv._1, kv._2)
    b.result
  }

  def -(key: K): collection.immutable.Map[K, V] = {
    val b = new SmallHashMap.Builder[K, V](size - 1)
    foreachItem { (k, v) =>
      if (key != k) b.add(k, v)
    }
    b.result
  }

  def ++(m: Map[K, V]): collection.immutable.Map[K, V] = {
    val b = new SmallHashMap.Builder[K, V](size + m.size)
    b.addAll(this)
    b.addAll(m)
    b.result
  }

  /** Constant time operation to get the number of pairs in the map. */
  override def size: Int = dataLength

  /**
    * Overridden to get better performance. See SmallHashMapHashCode benchmark for a
    * comparison with the default for various inputs.
    */
  override def hashCode: Int = {
    // Pattern copied from String.java of jdk
    if (cachedHashCode == 0) {
      cachedHashCode = computeHashCode
    }
    cachedHashCode
  }

  /**
    * Compute the hash code for the map. This method is based on the
    * [[scala.util.hashing.MurmurHash3.unorderedHash()]] method. It is more efficient
    * for our purposes because it avoids creating tons of [[scala.runtime.IntRef]]
    * objects as well as tuples during iteration.
    */
  private[util] def computeHashCode: Int = {
    var a, b = 0
    var c = 1
    var i = 0
    while (i < data.length) {
      if (data(i) != null) {
        val h = data(i).hashCode
        a += h
        b ^= h
        if (h != 0) c *= h
      }
      i += 1
    }
    var h = 0x3c074a61
    h = scala.util.hashing.MurmurHash3.mix(h, a)
    h = scala.util.hashing.MurmurHash3.mix(h, b)
    h = scala.util.hashing.MurmurHash3.mixLast(h, c)
    scala.util.hashing.MurmurHash3.finalizeHash(h, dataLength)
  }

  /**
    * Overridden to get better performance. See SmallHashMapEquals benchmark for a
    * comparison with the default for various inputs.
    */
  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    obj match {
      case m: SmallHashMap[_, _] =>
        // The hashCode is cached for this class and will often be a cheaper way to
        // exclude equality.
        if (this eq m) return true
        size == m.size && hashCode == m.hashCode && dataEquals(m.asInstanceOf[SmallHashMap[K, V]])
      case m: Map[_, _] =>
        super.equals(obj)
      case _ =>
        false
    }
  }

  /**
    * Compares the data arrays of the two maps. It is assumed that cheaper checks such
    * as the sizes of the arrays have already been resolved before this method is called.
    * This method will loop through the array and compare corresponding entries. If the
    * keys do not match, then it will do a lookup in the other map to check for corresponding
    * values.
    *
    * In practice, the maps are often being created from the same input source and therefore
    * have the same insertion order. In those cases the array equality will work fine and no
    * lookups will be needed.
    */
  private[util] def dataEquals(m: SmallHashMap[K, V]): Boolean = {
    var i = 0
    while (i < data.length) {
      val k1 = data(i).asInstanceOf[K]
      val k2 = m.data(i).asInstanceOf[K]

      if (k1 == k2) {
        val v1 = data(i + 1)
        val v2 = m.data(i + 1)
        if (v1 != v2) return false
      } else {
        if (!keyEquals(m, k1) || !keyEquals(m, k2)) return false
      }

      i += 2
    }
    true
  }

  private def keyEquals(m: SmallHashMap[K, V], k: K): Boolean = {
    if (k == null) true else {
      val v1 = getOrNull(k)
      val v2 = m.getOrNull(k)
      v1 == v2
    }
  }

  /** This is here to allow for testing and benchmarks. Should note be used otherwise. */
  private[util] def superEquals(obj: Any): Boolean = super.equals(obj)
}
