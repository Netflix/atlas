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

import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Tag

import java.lang

/**
  * Immutable map implementation for tag maps using a sorted array as the underlying storage.
  */
final class SortedTagMap private (private val data: Array[String], private val length: Int)
    extends scala.collection.immutable.Map[String, String]
    with Comparable[SortedTagMap] {

  private[this] var cachedHashCode: Int = 0

  private def find(key: String): Int = {
    if (length == 0) {
      -2
    } else {
      var left = 0
      var right = length / 2 - 1
      while (left <= right) {
        val mid = (left + right) / 2
        val cmp = data(mid * 2).compareTo(key)
        if (cmp == 0) {
          // equal, return position
          return mid * 2
        } else if (cmp > 0) {
          // key is in left half
          right = mid - 1
        } else {
          // key is in right half
          left = mid + 1
        }
      }
      // key is not present, return position for insertion
      -(left * 2) - 2
    }
  }

  override def removed(key: String): Map[String, String] = {
    val pos = find(key)
    if (pos < 0) this
    else {
      val copy = new Array[String](length - 2)
      System.arraycopy(data, 0, copy, 0, pos)
      System.arraycopy(data, pos + 2, copy, pos, length - pos - 2)
      new SortedTagMap(copy, copy.length)
    }
  }

  override def updated[V1 >: String](key: String, value: V1): Map[String, V1] = {
    val pos = find(key)
    if (pos < 0) {
      val insertPos = -(pos + 2)
      val copy = new Array[String](length + 2)
      System.arraycopy(data, 0, copy, 0, insertPos)
      copy(insertPos) = key
      copy(insertPos + 1) = value.asInstanceOf[String]
      System.arraycopy(data, insertPos, copy, insertPos + 2, length - insertPos)
      new SortedTagMap(copy, copy.length)
    } else {
      val copy = new Array[String](length)
      System.arraycopy(data, 0, copy, 0, length)
      copy(pos + 1) = value.asInstanceOf[String]
      new SortedTagMap(copy, copy.length)
    }
  }

  /** Return the key in the given position. */
  def key(i: Int): String = {
    data(i * 2)
  }

  /** Return the value in the given position. */
  def value(i: Int): String = {
    data(i * 2 + 1)
  }

  /**
    * Return the value associated with a key or null if it is not present. This method can be
    * useful to avoid the allocation of a tuple instance when using `get(key)`.
    */
  def getOrNull(key: String): String = {
    val pos = find(key)
    if (pos < 0) null else data(pos + 1)
  }

  override def get(key: String): Option[String] = {
    Option(getOrNull(key))
  }

  override def contains(key: String): Boolean = {
    getOrNull(key) != null
  }

  override def iterator: Iterator[(String, String)] = {
    val n = length
    new Iterator[(String, String)] {
      private var i = 0

      override def hasNext: Boolean = i < n

      override def next(): (String, String) = {
        val tuple = data(i) -> data(i + 1)
        i += 2
        tuple
      }
    }
  }

  override def foreachEntry[U](f: (String, String) => U): Unit = {
    var i = 0
    while (i < length) {
      f(data(i), data(i + 1))
      i += 2
    }
  }

  override def size: Int = {
    length / 2
  }

  /**
    * Returns the size of the backing array. Since the array has keys and values, the array
    * size will be twice the number of tuples it can hold.
    */
  private[util] def backingArraySize: Int = {
    data.length
  }

  /**
    * Overridden to get better performance.
    */
  override def hashCode: Int = {
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
    while (i < length) {
      val h = data(i).hashCode
      a += h
      b ^= h
      if (h != 0) c *= h
      i += 1
    }
    var h = 0x3C074A61
    h = scala.util.hashing.MurmurHash3.mix(h, a)
    h = scala.util.hashing.MurmurHash3.mix(h, b)
    h = scala.util.hashing.MurmurHash3.mixLast(h, c)
    scala.util.hashing.MurmurHash3.finalizeHash(h, length)
  }

  /**
    * Overridden to get better performance.
    */
  override def equals(other: Any): Boolean = {
    other match {
      case m: SortedTagMap => arrayEquals(data, length, m.data, m.length)
      case o               => super.equals(o)
    }
  }

  private def arrayEquals(
    a1: Array[String],
    length1: Int,
    a2: Array[String],
    length2: Int
  ): Boolean = {
    if (length1 != length2)
      return false
    var i = 0
    while (i < length1) {
      if (a1(i) != a2(i)) return false
      i += 1
    }
    true
  }

  override def compareTo(other: SortedTagMap): Int = {
    if (this eq other) {
      0
    } else {
      val n = math.min(size, other.size)
      var i = 0
      while (i < n) {
        var cmp = key(i).compareTo(other.key(i))
        if (cmp != 0)
          return cmp

        cmp = value(i).compareTo(other.value(i))
        if (cmp != 0)
          return cmp
        i += 1
      }
      // If they are equal up to this point, then remaining items in one list should
      // put it after the other
      size - other.size
    }
  }

  /**
    * Copy internal data for the map into a string array. The passed in buffer must have
    * a length of at least twice the size of this map.
    */
  def copyToArray(buffer: Array[String]): Unit = {
    System.arraycopy(data, 0, buffer, 0, length)
  }

  /** Return a sorted array containing the keys for this map. */
  def keysArray: Array[String] = {
    val ks = new Array[String](size)
    var i = 0
    while (i < ks.length) {
      ks(i) = key(i)
      i += 1
    }
    ks
  }

  /** Return an array containing the values in order based on the keys. */
  def valuesArray: Array[String] = {
    val vs = new Array[String](size)
    var i = 0
    while (i < vs.length) {
      vs(i) = value(i)
      i += 1
    }
    vs
  }

  /**
    * Returns a view of this map as a Spectator Id. For read only use-cases, such as using with
    * the Spectator QueryIndex, this can be more efficient than converting to the default
    * implementations of Id.
    *
    * @param defaultName
    *     Default value to use for the required name dimension on the id if that key is not
    *     present in the map. If set to `None`, then it will fail if there is no `name` key in
    *     the map.
    */
  def toSpectatorId(defaultName: Option[String] = None): Id = {
    val pos = find("name")
    if (pos < 0) {
      defaultName match {
        case Some(name) => Id.unsafeCreate(name, data, length)
        case None       => throw new IllegalArgumentException(s"`name` key is not present: $this")
      }
    } else {
      new SortedTagMap.IdView(this, pos / 2)
    }
  }
}

/** Helper functions for working with sorted tag maps. */
object SortedTagMap {

  /** Instance of empty tag map that can be reused. */
  val empty: SortedTagMap = new SortedTagMap(new Array[String](0), 0)

  /**
    * Create a new instance based on an array of paired strings. The array that is passed
    * in will not be modified and is not used internally so it is safe to reuse.
    */
  def apply(data: Array[String]): SortedTagMap = {
    val array = java.util.Arrays.copyOf(data, data.length)
    ArrayHelper.sortPairs(array)
    val length = dedup(array, array.length)
    new SortedTagMap(array, length)
  }

  /**
    * Create a new instance from varargs tuples.
    */
  def apply(data: (String, String)*): SortedTagMap = {
    val size = data.knownSize
    builder(if (size >= 0) size else 10).addAll(data).result()
  }

  /**
    * Create a new instance based on a collection of tuples.
    */
  def apply(data: IterableOnce[(String, String)]): SortedTagMap = {
    data match {
      case m: SortedTagMap => m
      case m: Map[String, String] =>
        builder(m.size).addAll(m).result()
      case _ =>
        val size = data.knownSize
        builder(if (size >= 0) size else 10).addAll(data).result()
    }
  }

  /**
    * Create a new instance from an already sorted and deduped array. The array will be used
    * internally to the map and should not be modified.
    */
  def createUnsafe(data: Array[String], length: Int): SortedTagMap = {
    new SortedTagMap(data, length)
  }

  /**
    * Dedup any entries in the array that have the same key. The last entry with a given
    * key will get selected. Input data must already be sorted by the tag key. Returns the
    * length of the overall deduped array.
    */
  private def dedup(data: Array[String], len: Int): Int = {
    if (len == 0) {
      0
    } else {
      var k = data(0)
      var i = 2
      var j = 0
      while (i < len) {
        if (k == data(i)) {
          data(j) = data(i)
          data(j + 1) = data(i + 1)
        } else {
          j += 2 // Not deduping, skip over previous entry
          k = data(i)
          if (i > j) {
            data(j) = k
            data(j + 1) = data(i + 1)
          }
        }
        i += 2
      }
      j + 2
    }
  }

  /**
    * Builder for constructing a new instance of a sorted tag map. The builder instance
    * cannot be used after the result is computed.
    *
    * @param initialSize
    *     Size to use for the buffer where added elements are placed. Setting this appropriately
    *     can improve efficiency by avoiding resizes of the buffer.
    * @return
    *     Instance of the builder.
    */
  def builder(initialSize: Int = 10): Builder = {
    new Builder(initialSize)
  }

  /** Builder for constructing a new instance of a sorted tag map. */
  class Builder(initialSize: Int) {

    private var buf = new Array[String](initialSize * 2)
    private var pos = 0

    private def resize(): Unit = {
      val tmp = new Array[String](buf.length + initialSize * 2)
      System.arraycopy(buf, 0, tmp, 0, pos)
      buf = tmp
    }

    /** Add a tuple into the tag map. */
    def +=(pair: (String, String)): Unit = add(pair._1, pair._2)

    /** Add a key/value pair into the tag map. */
    def add(key: String, value: String): Builder = {
      if (pos >= buf.length) {
        resize()
      }
      buf(pos) = key
      buf(pos + 1) = value
      pos += 2
      this
    }

    /** Add all entries from the map into the tag map. */
    def addAll(m: Map[String, String]): Builder = {
      m.foreachEntry(add)
      this
    }

    /** Add all tuples from the collection into the tag map. */
    def addAll(data: IterableOnce[(String, String)]): Builder = {
      val it = data.iterator
      while (it.hasNext) {
        this += it.next()
      }
      this
    }

    /**
      * Compute result and clear the internal buffer. The builder instance cannot be used again
      * after calling.
      */
    def result(): SortedTagMap = {
      ArrayHelper.sortPairs(buf, pos)
      val length = dedup(buf, pos)
      val map = createUnsafe(buf, length)
      buf = null
      map
    }

    /**
      * Same as result except that it will ensure the backing array is sized to exactly
      * fit the data.
      */
    def compact(): SortedTagMap = {
      ArrayHelper.sortPairs(buf, pos)
      val length = dedup(buf, pos)
      if (length < buf.length) {
        val tmp = new Array[String](length)
        System.arraycopy(buf, 0, tmp, 0, length)
        val map = createUnsafe(tmp, length)
        buf = null
        map
      } else {
        val map = createUnsafe(buf, length)
        buf = null
        map
      }
    }
  }

  /** Wraps a SortedTagMap to conform to the Spectator Id interface. */
  private class IdView(map: SortedTagMap, namePos: Int) extends Id {

    override def name(): String = {
      map.value(namePos)
    }

    override def tags(): lang.Iterable[Tag] = {
      import scala.jdk.CollectionConverters.*
      map.toSeq.filter(_._1 != "name").map(t => Tag.of(t._1, t._2)).asJava
    }

    /**
      * For an Id, the first element is the name and then the rest of the tag list is
      * sorted. The SortedTagMap has everything sorted by the key. The method adjusts the
      * index used for an Id based on the position of the name key to that used by the
      * SortedTagMap.
      */
    private def computeIndex(i: Int): Int = {
      if (i == 0)
        namePos
      else if (i <= namePos)
        i - 1
      else
        i
    }

    override def getKey(i: Int): String = {
      map.key(computeIndex(i))
    }

    override def getValue(i: Int): String = {
      map.value(computeIndex(i))
    }

    override def size(): Int = {
      map.size
    }

    override def withTag(k: String, v: String): Id = {
      toDefaultId.withTag(k, v)
    }

    override def withTag(t: Tag): Id = {
      toDefaultId.withTag(t)
    }

    private def toDefaultId: Id = {
      Id.create(name()).withTags(tags())
    }
  }
}
