/*
 * Copyright 2014-2020 Netflix, Inc.
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

import java.util
import java.util.Comparator

import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.IntIntHashMap
import com.netflix.atlas.core.util.IntRefHashMap
import com.netflix.atlas.core.util.LongHashSet
import com.netflix.atlas.core.util.RefIntHashMap
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory

object RoaringTagIndex {
  private val logger = LoggerFactory.getLogger(getClass)

  def empty[T <: TaggedItem: Manifest]: RoaringTagIndex[T] = {
    new RoaringTagIndex(new Array[T](0), new IndexStats())
  }
}

/**
  * Create a new index based on roaring bitmaps.
  *
  * https://github.com/RoaringBitmap/RoaringBitmap
  *
  * @param items
  *     Items to include in the index.
  */
class RoaringTagIndex[T <: TaggedItem](items: Array[T], stats: IndexStats) extends TagIndex[T] {

  import com.netflix.atlas.core.index.RoaringTagIndex._

  type RoaringValueMap = IntRefHashMap[RoaringBitmap]
  type RoaringKeyMap = IntRefHashMap[RoaringValueMap]

  // Comparator for ordering tagged items using the id
  private val idComparator = new Comparator[T] {

    def compare(t1: T, t2: T): Int = t1.id.compareTo(t2.id)
  }

  // Precomputed set of all items
  private val all = {
    val set = new RoaringBitmap()
    set.add(0L, items.length)
    set
  }

  private val (keys, values) = {
    val keySet = new util.HashSet[String](items.length)
    val valueSet = new util.HashSet[String](items.length)
    var pos = 0
    while (pos < items.length) {
      items(pos).foreach { (k, v) =>
        keySet.add(k)
        valueSet.add(v)
      }
      pos += 1
    }
    val ks = keySet.toArray(new Array[String](keySet.size()))
    val vs = valueSet.toArray(new Array[String](valueSet.size()))
    util.Arrays.sort(ks.asInstanceOf[Array[AnyRef]])
    util.Arrays.sort(vs.asInstanceOf[Array[AnyRef]])
    (ks, vs)
  }

  private val keyMap = createPositionMap(keys)
  private val valueMap = createPositionMap(values)

  // Primary indexes to search for a tagged item:
  //
  // * itemIds: sorted array of item ids
  //
  // * itemIndex: key -> value -> set, the set contains indexes to the items array
  //
  // * keyIndex: key -> set, precomputed union of all sets for a given key
  //
  // * tagIndex: sorted array of all tags. The tags are stored as long values with
  //   the key as the most significant 32bits and the values as the least significant
  //   32bits. Key is a position in the keys array, value is a position in the values
  //   array.
  //
  // * itemTags: map of key value pairs for an item. The key and value numbers are positions
  //   to the keys and values arrays respectively.
  private val (itemIds, itemIndex, keyIndex, tagIndex, itemTags) = buildItemIndex()

  // Collect and log various index stats
  collectStats()

  private def collectStats(): Unit = {
    logger.info(s"items = ${items.length}, keys = ${keys.length}, values = ${values.length}")
    val builder = List.newBuilder[IndexStats.KeyStat]
    var i = 0
    while (i < keys.length) {
      val numValues = itemIndex.get(i).size
      val numItems = keyIndex.get(i).getCardinality
      builder += IndexStats.KeyStat(keys(i), numItems, numValues)
      i += 1
    }
    stats.updateKeyStats(builder.result())
    stats.updateIndexStats(items.length)
  }

  private def createPositionMap(data: Array[String]): RefIntHashMap[String] = {
    val m = new RefIntHashMap[String](2 * data.length)
    var i = 0
    while (i < data.length) {
      m.put(data(i), i)
      i += 1
    }
    m
  }

  private def buildItemIndex()
    : (Array[ItemId], RoaringKeyMap, RoaringValueMap, Array[Long], Array[IntIntHashMap]) = {

    // Sort items array based on the id, allows for efficient paging of requests using the id
    // as the offset
    logger.debug(s"building index with ${items.length} items, starting sort")
    util.Arrays.sort(items, idComparator)
    val itemIds = new Array[ItemId](items.length)

    // Build the main index
    logger.debug(s"building index with ${items.length} items, create main key map")
    val kidx = new RoaringValueMap(-1)
    val idx = new RoaringKeyMap(-1)
    val itemTags = new Array[IntIntHashMap](items.length)
    val tagsSet = new LongHashSet(-1L, items.length)
    var pos = 0
    while (pos < items.length) {
      itemIds(pos) = items(pos).id
      itemTags(pos) = new IntIntHashMap(-1, 2 * items(pos).tags.size)
      items(pos).foreach { (k, v) =>
        val kp = keyMap.get(k, -1)
        var vidx = idx.get(kp)
        if (vidx == null) {
          vidx = new RoaringValueMap(-1)
          idx.put(kp, vidx)
        }

        // Add to value index
        val vp = valueMap.get(v, -1)
        var matchSet = vidx.get(vp)
        if (matchSet == null) {
          matchSet = new RoaringBitmap()
          vidx.put(vp, matchSet)
        }
        matchSet.add(pos)

        // Add to key index
        matchSet = kidx.get(kp)
        if (matchSet == null) {
          matchSet = new RoaringBitmap()
          kidx.put(kp, matchSet)
        }
        matchSet.add(pos)

        itemTags(pos).put(kp, vp)

        val t = (kp.toLong << 32) | vp.toLong
        tagsSet.add(t)
      }
      pos += 1
    }

    val tagsArray = new Array[Long](tagsSet.size)
    var i = 0
    tagsSet.foreach { t =>
      tagsArray(i) = t
      i += 1
    }
    util.Arrays.sort(tagsArray)

    (itemIds, idx, kidx, tagsArray, itemTags)
  }

  /**
    * Encode a tag as a long value. The first 32-bits are the key and the last 32-bits
    * are the value.
    */
  private def tag(k: Int, v: Int): Long = (k.toLong << 32) | v.toLong

  /** Extract the key for an encoded tag. */
  private def tagKey(t: Long): Int = (t >> 32).toInt

  /** Extract the value for an encoded tag. */
  private def tagValue(t: Long): Int = (t & 0X00000000FFFFFFFFL).toInt

  private[index] def findImpl(query: Query, offset: Int): RoaringBitmap = {
    import com.netflix.atlas.core.model.Query._
    query match {
      case And(q1, q2)            => and(q1, q2, offset)
      case Or(q1, q2)             => or(q1, q2, offset)
      case Not(q)                 => diff(all, findImpl(q, offset))
      case Equal(k, v)            => equal(k, v, offset)
      case GreaterThan(k, v)      => greaterThan(k, v, false)
      case GreaterThanEqual(k, v) => greaterThan(k, v, true)
      case LessThan(k, v)         => lessThan(k, v, false)
      case LessThanEqual(k, v)    => lessThan(k, v, true)
      case q: In                  => findImpl(q.toOrQuery, offset)
      case q: PatternQuery        => strPattern(q, offset)
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
    val s1 = findImpl(q1, offset)
    if (s1.isEmpty) s1
    else {
      // Short circuit, only perform second query if s1 is not empty
      val s2 = findImpl(q2, offset)
      s1.and(s2)
      s1
    }
  }

  private def or(q1: Query, q2: Query, offset: Int): RoaringBitmap = {
    val s1 = findImpl(q1, offset)
    val s2 = findImpl(q2, offset)
    s1.or(s2)
    s1
  }

  private def equal(k: String, v: String, offset: Int): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val vp = valueMap.get(v, -1)
      val matchSet = vidx.get(vp)
      if (matchSet == null) new RoaringBitmap() else withOffset(matchSet, offset)
    }
  }

  private def greaterThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new RoaringBitmap()
      val vp = findOffset(values, v, if (orEqual) 0 else 1)
      val t = tag(kp, vp)
      var i = tagOffset(t)

      // Data is sorted, no need to perform a check for each entry if key matches
      while (i < tagIndex.length && tagKey(tagIndex(i)) == kp) {
        set.or(vidx.get(tagValue(tagIndex(i))))
        i += 1
      }
      set
    }
  }

  private def lessThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new RoaringBitmap()
      val vp = findOffset(values, v, if (orEqual) 0 else -1)
      val t = tag(kp, vp)
      var i = tagOffset(t)

      // Data is sorted, no need to perform a check for each entry if key matches
      while (i >= 0 && tagKey(tagIndex(i)) == kp) {
        set.or(vidx.get(tagValue(tagIndex(i))))
        i -= 1
      }
      set
    }
  }

  private def strPattern(q: Query.PatternQuery, offset: Int): RoaringBitmap = {
    val kp = keyMap.get(q.k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new RoaringBitmap()
      val prefix = q.pattern.prefix()
      if (prefix != null) {
        val vp = findOffset(values, prefix, 0)
        val t = tag(kp, vp)
        var i = tagOffset(t)
        while (i < tagIndex.length
               && tagKey(tagIndex(i)) == kp
               && values(tagValue(tagIndex(i))).startsWith(prefix)) {
          val v = tagValue(tagIndex(i))
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
    val matchSet = keyIndex.get(kp)
    if (matchSet == null) new RoaringBitmap() else withOffset(matchSet, offset)
  }

  private def itemOffset(v: String): Int = {
    if (v == null || v == "") 0
    else {
      val offsetV = ItemId(v)
      val pos = util.Arrays.binarySearch(itemIds.asInstanceOf[Array[AnyRef]], offsetV)
      if (pos < 0) -pos - 1 else pos
    }
  }

  private def tagOffset(v: Long): Int = {
    if (v <= 0) 0
    else {
      val pos = util.Arrays.binarySearch(tagIndex, v)
      if (pos == -1) 0 else if (pos < -1) -pos - 1 else pos
    }
  }

  /**
    * Find the offset for `v` in the array `vs`. If an exact match is found, then
    * the value `n` will be added to the position. This is mostly used for skipping
    * equal values in the case of strictly less than or greater than comparisons. By
    * default a greater than, `n = 1`, comparison will be done.
    */
  private def findOffset(vs: Array[String], v: String, n: Int = 1): Int = {
    if (v == null || v == "") 0
    else {
      val pos = util.Arrays.binarySearch(vs.asInstanceOf[Array[AnyRef]], v)
      if (pos >= 0) pos + n else -pos - 1
    }
  }

  def findTags(query: TagQuery): List[Tag] = {
    val k = query.key
    if (k.isDefined) {
      findValues(query.copy(offset = query.offsetTag.value)).map(v => Tag(k.get, v))
    } else {
      Nil
    }
  }

  def findKeys(query: TagQuery): List[String] = {
    if (query.query.isEmpty) {
      val offset = findOffset(keys, query.offset)
      val builder = List.newBuilder[String]
      var i = offset
      val end = if (keys.length - i > query.limit) i + query.limit else keys.length
      while (i < end) {
        builder += keys(i)
        i += 1
      }
      builder.result()
    } else {
      val q = query.query.getOrElse(Query.True)
      val itemSet = findImpl(q, 0)
      val offset = findOffset(keys, query.offset)

      val results = new util.BitSet(keys.length)
      val iter = itemSet.getIntIterator
      while (iter.hasNext) {
        val tags = itemTags(iter.next())
        tags.foreach { (k, _) =>
          if (k >= offset) results.set(k)
        }
      }

      createResultList(keys, results, query.limit)
    }
  }

  def findValues(query: TagQuery): List[String] = {
    require(query.key.isDefined)
    val k = query.key.get
    val kp = keyMap.get(k, -1)
    if (kp < 0) return Nil

    val has = Query.HasKey(k)
    val q = query.query.fold[Query](has)(q => q.and(has))
    val itemSet = findImpl(q, 0)
    val offset = findOffset(values, query.offset)

    val results = new util.BitSet(values.length)

    // If there are many items with the same value for a key, then we can prune the item set
    // by doing an AND NOT operation with set for that key and value. This will perform worse
    // if there aren't a lot of items with the same value. We estimate the chances by comparing
    // the cardinality of the key with the number of items. For less than 5% it is assumed that
    // pruning is the better option.
    val keyCardinality = itemIndex.get(kp).size
    val attemptPruning = (100.0 * keyCardinality / items.length) < 5.0

    // Find all matching values by looking up the key for each matching item
    var iter = itemSet.getIntIterator
    while (iter.hasNext) {
      val i = iter.next()
      val tags = itemTags(i)
      val v = tags.get(kp, -1)
      if (v >= offset && !results.get(v)) {
        results.set(v)
      } else if (v >= 0 && attemptPruning) {
        // If the value is repeated, then lookup the set of all items with the given value
        // for the key and removing those from the item set
        itemSet.andNot(equal(kp, v))
        iter = itemSet.getIntIterator
      }
    }

    createResultList(values, results, query.limit)
  }

  private def equal(k: Int, v: Int): RoaringBitmap = {
    val vidx = itemIndex.get(k)
    if (vidx == null) new RoaringBitmap()
    else {
      val matchSet = vidx.get(v)
      if (matchSet == null) new RoaringBitmap() else matchSet
    }
  }

  def findItems(query: TagQuery): List[T] = {
    val offset = itemOffset(query.offset)
    val limit = query.limit
    val intSet = query.query.fold(withOffset(all, offset))(q => findImpl(q, offset))
    createResultList(items, intSet, limit)
  }

  private def createResultList(
    vs: Array[String],
    matches: util.BitSet,
    limit: Int
  ): List[String] = {
    val result = List.newBuilder[String]
    var i = matches.nextSetBit(0)
    var count = 0
    while (i >= 0 && count < limit) {
      result += vs(i)
      i = matches.nextSetBit(i + 1)
      count += 1
    }
    result.result()
  }

  private def createResultList[V](vs: Array[V], matches: RoaringBitmap, limit: Int): List[V] = {
    val result = List.newBuilder[V]
    val iter = matches.getIntIterator
    var count = 0
    while (iter.hasNext && count < limit) {
      result += vs(iter.next())
      count += 1
    }
    result.result()
  }

  val size: Int = items.length
}
