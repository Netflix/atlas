/*
 * Copyright 2014-2021 Netflix, Inc.
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
  * Immutable map implementation for tag maps using a sorted array as the underlying storage.
  */
final class SortedTagMap private (data: Array[String], length: Int)
    extends scala.collection.immutable.Map[String, String]
    with Comparable[SortedTagMap] {

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
    new SortedTagMap(array, array.length)
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
    * Create a new instance from an already sorted array. The array will be reused and should
    * not be modified.
    */
  def createUnsafe(data: Array[String], length: Int): SortedTagMap = {
    new SortedTagMap(data, length)
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
      val map = createUnsafe(buf, pos)
      buf = null
      map
    }
  }
}
