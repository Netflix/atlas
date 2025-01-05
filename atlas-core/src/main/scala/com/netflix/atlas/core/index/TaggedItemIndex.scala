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
package com.netflix.atlas.core.index

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.LongHashSet
import com.netflix.atlas.core.util.RefIntHashMap
import org.roaringbitmap.RoaringBitmap

/**
  * Index for efficiently finding tagged items that match a query.
  *
  * @param keyMap
  *     Maps the key string to the numeric value used internally to other index
  *     data structures.
  * @param values
  *     Sorted array of all strings used as tag values.
  * @param valueMap
  *     Maps the value string to the numeric value used internally to other index
  *     data structures.
  * @param all
  *     Bit set representing all items in the index.
  * @param hasKeyIdx
  *     Map from key to set of items that have that key in the tag set.
  * @param keyValueIdx
  *     Map from key to value to bit set of items that have the key and value in their
  *     tag set.
  * @param tagPositions
  *     Sorted array of all tags. The tags are stored as long values with
  *     the key as the most significant 32bits and the values as the least significant
  *     32bits. Key is a position in the keys array, value is a position in the values
  *     array.
  */
class TaggedItemIndex private (
  keyMap: RefIntHashMap[String],
  values: Array[String],
  valueMap: RefIntHashMap[String],
  all: RoaringBitmap,
  hasKeyIdx: ValueIndex,
  keyValueIdx: KeyValueIndex,
  tagPositions: Array[Long]
) {

  /**
    * Find all items that match the provided query.
    *
    * @param query
    *     Query to use for finding matching items.
    * @param offset
    *     Position offset for where to start with results. This can be used by the caller
    *     to ignore earlier items that have already been accessed when paginating.
    * @return
    *     Bit set indicating the positions of the matching items.
    */
  def find(query: Query, offset: Int = 0): RoaringBitmap = {
    import com.netflix.atlas.core.model.Query.*
    query match {
      case And(q1, q2)            => and(q1, q2, offset)
      case Or(q1, q2)             => or(q1, q2, offset)
      case Not(q)                 => diff(all, find(q, offset))
      case Equal(k, v)            => equal(k, v, offset)
      case GreaterThan(k, v)      => greaterThan(k, v, false)
      case GreaterThanEqual(k, v) => greaterThan(k, v, true)
      case LessThan(k, v)         => lessThan(k, v, false)
      case LessThanEqual(k, v)    => lessThan(k, v, true)
      case q: In                  => find(q.toOrQuery, offset)
      case q: PatternQuery        => strPattern(q)
      case HasKey(k)              => hasKey(k, offset)
      case True                   => all.clone()
      case False                  => new RoaringBitmap()
    }
  }

  private def diff(s1: RoaringBitmap, s2: RoaringBitmap): RoaringBitmap = {
    val s = s1.clone()
    s.andNot(s2)
    s
  }

  private def withOffset(set: RoaringBitmap, offset: Int): RoaringBitmap = {
    val s = set.clone()
    if (offset > 0) s.remove(0L, offset + 1L)
    s
  }

  private def and(q1: Query, q2: Query, offset: Int): RoaringBitmap = {
    val s1 = find(q1, offset)
    if (s1.isEmpty) s1
    else {
      // Short circuit, only perform second query if s1 is not empty
      val s2 = find(q2, offset)
      s1.and(s2)
      s1
    }
  }

  private def or(q1: Query, q2: Query, offset: Int): RoaringBitmap = {
    val s1 = find(q1, offset)
    val s2 = find(q2, offset)
    s1.or(s2)
    s1
  }

  private def equal(k: String, v: String, offset: Int): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = keyValueIdx.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val vp = valueMap.get(v, -1)
      val matchSet = vidx.get(vp)
      if (matchSet == null) new RoaringBitmap() else withOffset(matchSet, offset)
    }
  }

  private def greaterThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = keyValueIdx.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new RoaringBitmap()
      val vp = findOffset(values, v, if (orEqual) 0 else 1)
      val t = tag(kp, vp)
      var i = tagOffset(t)

      // Data is sorted, no need to perform a check for each entry if key matches
      while (i < tagPositions.length && tagKey(tagPositions(i)) == kp) {
        set.or(vidx.get(tagValue(tagPositions(i))))
        i += 1
      }
      set
    }
  }

  private def lessThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = keyValueIdx.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new RoaringBitmap()
      val vp = findOffset(values, v, if (orEqual) 0 else -1)
      val t = tag(kp, vp)
      var i = tagOffset(t)

      // Data is sorted, no need to perform a check for each entry if key matches
      while (i >= 0 && tagKey(tagPositions(i)) == kp) {
        set.or(vidx.get(tagValue(tagPositions(i))))
        i -= 1
      }
      set
    }
  }

  private def strPattern(q: Query.PatternQuery): RoaringBitmap = {
    val kp = keyMap.get(q.k, -1)
    val vidx = keyValueIdx.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new RoaringBitmap()
      val prefix = q.pattern.prefix()
      if (prefix != null) {
        val vp = findOffset(values, prefix, 0)
        val t = tag(kp, vp)
        var i = tagOffset(t)
        while (
          i < tagPositions.length
          && tagKey(tagPositions(i)) == kp
          && values(tagValue(tagPositions(i))).startsWith(prefix)
        ) {
          val v = tagValue(tagPositions(i))
          if (q.check(values(v))) {
            set.or(vidx.get(v))
          }
          i += 1
        }
      } else {
        vidx.foreach { (v, items) =>
          if (q.check(values(v)))
            set.or(items)
        }
      }
      set
    }
  }

  private def hasKey(k: String, offset: Int): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val matchSet = hasKeyIdx.get(kp)
    if (matchSet == null) new RoaringBitmap() else withOffset(matchSet, offset)
  }

  private def tagOffset(v: Long): Int = {
    if (v <= 0) 0
    else {
      val pos = java.util.Arrays.binarySearch(tagPositions, v)
      if (pos == -1) 0 else if (pos < -1) -pos - 1 else pos
    }
  }

  /**
    * Find the offset for `v` in the array `vs`. If an exact match is found, then
    * the value `n` will be added to the position. This is mostly used for skipping
    * equal values in the case of strictly less than or greater than comparisons.
    */
  private def findOffset(vs: Array[String], v: String, n: Int): Int = {
    if (v == null || v == "") 0
    else {
      val pos = java.util.Arrays.binarySearch(vs.asInstanceOf[Array[AnyRef]], v)
      if (pos >= 0) pos + n else -pos - 1
    }
  }

  /**
    * Encode a tag as a long value. The first 32-bits are the key and the last 32-bits
    * are the value.
    */
  private def tag(k: Int, v: Int): Long = (k.toLong << 32) | v.toLong

  /** Extract the key for an encoded tag. */
  private def tagKey(t: Long): Int = (t >> 32).toInt

  /** Extract the value for an encoded tag. */
  private def tagValue(t: Long): Int = (t & 0x00000000FFFFFFFFL).toInt
}

object TaggedItemIndex {

  /**
    * Create an index from the provided sequence. The items should already be in the desired
    * order for receiving results.
    */
  def apply(items: Seq[? <: TaggedItem]): TaggedItemIndex = {
    val builder = new Builder(items.size)
    items.foreach { item =>
      item.foreach(builder.addStrings)
    }
    builder.createStringTables()
    items.zipWithIndex.foreach {
      case (item, i) =>
        item.foreach { (k, v) =>
          builder.addTag(i, k, v)
        }
    }
    builder.build()
  }

  /**
    * Builder for creating an index. The builder should be used in a particular order. A
    * first pass over the tags is needed to compute a table of strings. A subsequent pass
    * is needed to build up the actual index. Steps:
    *
    * 1. Call `addStrings` for each tag key/value pair for all items.
    * 2. Call `createStringTable` to build the string tables.
    * 3. Call `addTag` for each tag key/value pair for all items.
    * 4. Call `build` to create the final index.
    *
    * @param size
    *     Number of items that will be in the index.
    */
  class Builder(size: Int) {

    private var keySet = new java.util.HashSet[String]
    private var valueSet = new java.util.HashSet[String]

    private var keys: Array[String] = _
    private var values: Array[String] = _

    private var keyMap: RefIntHashMap[String] = _
    private var valueMap: RefIntHashMap[String] = _

    private var hasKeyIdx: ValueIndex = _
    private var keyValueIdx: KeyValueIndex = _

    private val tagsSet = new LongHashSet(-1L, size)

    /** Add a string key and value into the string tables. */
    def addStrings(k: String, v: String): Unit = {
      keySet.add(k)
      valueSet.add(v)
    }

    private def toArray(stringSet: java.util.HashSet[String]): Array[String] = {
      val strings = stringSet.toArray(new Array[String](stringSet.size()))
      java.util.Arrays.sort(strings.asInstanceOf[Array[AnyRef]])
      strings
    }

    private def createPositionMap(strings: Array[String]): RefIntHashMap[String] = {
      val posMap = new RefIntHashMap[String](2 * strings.length)
      var i = 0
      while (i < strings.length) {
        posMap.put(strings(i), i)
        i += 1
      }
      posMap
    }

    /** Create string tables and prepare for building index structures. */
    def createStringTables(): Unit = {
      keys = toArray(keySet)
      keyMap = createPositionMap(keys)
      keySet = null

      values = toArray(valueSet)
      valueMap = createPositionMap(values)
      valueSet = null

      hasKeyIdx = new ValueIndex(-1)
      keyValueIdx = new KeyValueIndex(-1)
    }

    /**
      * Add each of the tags.
      *
      * @param i
      *     Position for the item that has the tag.
      * @param k
      *     Key for the tag. The key must have already been passed in when building
      *     the string table.
      * @param v
      *     Value for the tag. The value must have already been passed in when building
      *     the string table.
      */
    def addTag(i: Int, k: String, v: String): Unit = {
      val kpos = keyMap.get(k, -1)
      val vpos = valueMap.get(v, -1)

      // Add to has key index
      var matchSet = hasKeyIdx.get(kpos)
      if (matchSet == null) {
        matchSet = new RoaringBitmap()
        hasKeyIdx.put(kpos, matchSet)
      }
      matchSet.add(i)

      // Add to key/value index
      var valueIdx = keyValueIdx.get(kpos)
      if (valueIdx == null) {
        valueIdx = new ValueIndex(-1)
        keyValueIdx.put(kpos, valueIdx)
      }
      matchSet = valueIdx.get(vpos)
      if (matchSet == null) {
        matchSet = new RoaringBitmap()
        valueIdx.put(vpos, matchSet)
      }
      matchSet.add(i)

      // Add to tags set
      val t = (kpos.toLong << 32) | vpos.toLong
      tagsSet.add(t)
    }

    def build(): TaggedItemIndex = {
      val all = new RoaringBitmap()
      all.add(0L, size)

      val tagPositions = tagsSet.toArray
      java.util.Arrays.sort(tagPositions)

      new TaggedItemIndex(keyMap, values, valueMap, all, hasKeyIdx, keyValueIdx, tagPositions)
    }
  }
}
