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
package com.netflix.atlas.core.index

import java.math.BigInteger
import java.util
import java.util.Comparator

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.IntHashSet
import com.netflix.atlas.core.util.IntIntHashMap
import com.netflix.atlas.core.util.Interner
import com.netflix.atlas.core.util.NoopInterner
import com.netflix.atlas.core.util.RefIntHashMap
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory


object RoaringTagIndex {
  private val logger = LoggerFactory.getLogger(getClass)

  def empty[T <: TaggedItem: Manifest]: RoaringTagIndex[T] = {
    new RoaringTagIndex(new Array[T](0))
  }
}

/**
  * Create a new index based on roaring bitmaps.
  *
  * https://github.com/RoaringBitmap/RoaringBitmap
  *
  * @param items
  *     Items to include in the index.
  * @param internWhileBuilding
  *     Should strings get interned while building the index? This should be true unless
  *     it is known that strings have been interned before being added to the index.
  * @param interner
  *     Interner used to ensure we do not have duplicate string data. Internally there
  *     are usages of `java.util.IdentityHashMap` so we must have a unique copy of each
  *     string.
  */
class RoaringTagIndex[T <: TaggedItem](
  items: Array[T],
  internWhileBuilding: Boolean = true,
  interner: Interner[String] = Interner.forStrings)
  extends TagIndex[T] {

  import com.netflix.atlas.core.index.RoaringTagIndex._

  type RoaringValueMap = util.IdentityHashMap[String, RoaringBitmap]
  type RoaringKeyMap = util.IdentityHashMap[String, RoaringValueMap]

  type ValueMap = util.IdentityHashMap[String, IntHashSet]
  type KeyMap = util.IdentityHashMap[String, ValueMap]

  // Interner to use for building the index
  private val buildInterner = if (internWhileBuilding) interner else new NoopInterner[String]

  // Comparator for ordering tagged items using the id
  private val idComparator = new Comparator[T] {
    def compare(t1: T, t2: T): Int = t1.id compareTo t2.id
  }

  // Precomputed set of all items
  private val all = {
    val set = new RoaringBitmap()
    set.add(0L, items.length)
    set
  }

  // Primary indexes to search for a tagged item:
  // * itemIds: sorted array of item ids
  // * itemIndex: key -> value -> set, the set contains indexes to the items array
  // * keyIndex: key -> set, precomputed union of all sets for a given key
  private val (itemIds, itemIndex, keyIndex) = buildItemIndex()

  // Indexes to search for matching tags
  // * tagsIndex: sorted array of all unique tags and overall counts
  // * itemTags: itemTags(i) has an array of indexes into tags array for items(i)
  private val (tagIndex, itemTags) = buildTagIndex()

  private def newRoaringSet(vs: IntHashSet): RoaringBitmap = {
    val set = new RoaringBitmap()
    vs.foreach { v => set.add(v) }
    set
  }

  private def buildItemIndex(): (Array[BigInteger], RoaringKeyMap, RoaringValueMap) = {
    // Sort items array based on the id, allows for efficient paging of requests using the id
    // as the offset
    logger.debug(s"building index with ${items.length} items, starting sort")
    util.Arrays.sort(items, idComparator)
    val itemIds = new Array[BigInteger](items.length)

    // Build the main index
    logger.debug(s"building index with ${items.length} items, create main key map")
    val kidx = new ValueMap
    val idx = new KeyMap
    var pos = 0
    while (pos < items.length) {
      itemIds(pos) = items(pos).id
      items(pos).foreach { (k, v) =>
        val internedK = buildInterner.intern(k)
        var vidx = idx.get(internedK)
        if (vidx == null) {
          vidx = new ValueMap
          idx.put(internedK, vidx)
        }

        // Add to value index
        val internedV = buildInterner.intern(v)
        var matchSet = vidx.get(internedV)
        if (matchSet == null) {
          matchSet = new IntHashSet(-1, 20)
          vidx.put(internedV, matchSet)
        }
        matchSet.add(pos)

        // Add to key index
        matchSet = kidx.get(internedK)
        if (matchSet == null) {
          matchSet = new IntHashSet(-1, 20)
          kidx.put(internedK, matchSet)
        }
        matchSet.add(pos)
      }
      pos += 1
    }

    // Build final item index
    logger.debug(s"building index with ${items.length} items, create roaring index")
    val roaringIdx = new RoaringKeyMap
    val keys = idx.entrySet.iterator
    while (keys.hasNext) {
      val roaringVidx = new RoaringValueMap
      val keyEntry = keys.next()
      val values = keyEntry.getValue.entrySet.iterator
      while (values.hasNext) {
        val valueEntry = values.next()
        val roaringSet = newRoaringSet(valueEntry.getValue)
        roaringVidx.put(valueEntry.getKey, roaringSet)
      }
      roaringIdx.put(keyEntry.getKey, roaringVidx)
    }

    // Build final key index
    logger.debug(s"building index with ${items.length} items, create key index")
    val roaringKidx = new RoaringValueMap
    val keyIter = kidx.entrySet.iterator
    while (keyIter.hasNext) {
      val keyEntry = keyIter.next()
      val roaringSet = newRoaringSet(keyEntry.getValue)
      roaringKidx.put(keyEntry.getKey, roaringSet)
    }

    (itemIds, roaringIdx, roaringKidx)
  }

  private def buildTagIndex(): (Array[Tag], Array[Array[Int]]) = {
    // Count how many times each tag occurs
    logger.debug(s"building tag index with ${items.length} items, compute tag counts")
    val tagCounts = new scala.collection.mutable.AnyRefMap[String, RefIntHashMap[String]]
    var pos = 0
    while (pos < items.length) {
      items(pos).foreach { (k, v) =>
        tagCounts.getOrElseUpdate(k, new RefIntHashMap[String]()).increment(v, 1)
      }
      pos += 1
    }

    // Create sorted array with tags and the overall counts
    logger.debug(s"building tag index with ${items.length} items, sort and overall counts")
    val tagsSize = tagCounts.map(_._2.size).sum
    val tagsArray = new Array[Tag](tagsSize)
    pos = 0
    tagCounts.foreach { case (k, vc) =>
      vc.foreach { (v, c) =>
        tagsArray(pos) = Tag(buildInterner.intern(k), buildInterner.intern(v), c)
        pos += 1
      }
    }
    util.Arrays.sort(tagsArray.asInstanceOf[Array[AnyRef]])

    // Create map of tag to position in tags array
    logger.debug(s"building tag index with ${items.length} items, tag to position map")
    val posMap = new scala.collection.mutable.AnyRefMap[String, RefIntHashMap[String]]
    pos = 0
    while (pos < tagsArray.length) {
      val t = tagsArray(pos)
      posMap.getOrElseUpdate(t.key, new RefIntHashMap[String]()).put(t.value, pos)
      pos += 1
    }

    // Build array of the tags for a given item
    logger.debug(s"building tag index with ${items.length} items, create item to tag ints map")
    val itemTags = new Array[Array[Int]](items.length)
    pos = 0
    while (pos < items.length) {
      val tags = items(pos).tags
      val tagsArray = new Array[Int](tags.size)
      var i = 0
      items(pos).foreach { (k, v) =>
        tagsArray(i) = posMap(k).get(v, -1)
        i += 1
      }
      itemTags(pos) = tagsArray
      pos += 1
    }

    (tagsArray, itemTags)
  }

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
    if (s1.isEmpty) s1 else {
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
    val internedK = interner.intern(k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) new RoaringBitmap() else {
      val internedV = interner.intern(v)
      val matchSet = vidx.get(internedV)
      if (matchSet == null) new RoaringBitmap() else withOffset(matchSet, offset)
    }
  }

  private def greaterThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val internedK = interner.intern(k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) new RoaringBitmap() else {
      val set = new RoaringBitmap()
      val tag = Tag(internedK, v, -1)
      var i = tagOffset(tag)
      // Skip if equal
      if (!orEqual && i < tagIndex.length && tagIndex(i).key == internedK && tagIndex(i).value == v) {
        i += 1
      }
      // Data is sorted, no need to perform a check for each entry if key matches
      while (i < tagIndex.length && tagIndex(i).key == internedK) {
        set.or(vidx.get(tagIndex(i).value))
        i += 1
      }
      set
    }
  }

  private def lessThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val internedK = interner.intern(k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) new RoaringBitmap() else {
      val set = new RoaringBitmap()
      val tag = Tag(internedK, v, -1)
      var i = tagOffset(tag)
      // Skip if equal
      if (!orEqual && i >= 0 && tagIndex(i).key == internedK && tagIndex(i).value == v) {
        i -= 1
      }
      // Data is sorted, no need to perform a check for each entry if key matches
      while (i >= 0 && tagIndex(i).key == internedK) {
        set.or(vidx.get(tagIndex(i).value))
        i -= 1
      }
      set
    }
  }

  private def strPattern(q: Query.PatternQuery, offset: Int): RoaringBitmap = {
    val internedK = interner.intern(q.k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) new RoaringBitmap() else {
      val set = new RoaringBitmap()
      if (q.pattern.prefix.isDefined) {
        val prefix = q.pattern.prefix.get
        val tag = Tag(internedK, prefix, -1)
        var i = tagOffset(tag)
        while (i < tagIndex.length &&
          tagIndex(i).key == internedK &&
          tagIndex(i).value.startsWith(prefix)) {
          if (q.check(tagIndex(i).value)) {
            set.or(vidx.get(tagIndex(i).value))
          }
          i += 1
        }
      } else {
        val entries = vidx.entrySet.iterator
        while (entries.hasNext) {
          val entry = entries.next()
          if (q.check(entry.getKey))
            set.or(withOffset(entry.getValue, offset))
        }
      }
      set
    }
  }

  private def hasKey(k: String, offset: Int): RoaringBitmap = {
    val internedK = interner.intern(k)
    val matchSet = keyIndex.get(internedK)
    if (matchSet == null) new RoaringBitmap() else withOffset(matchSet, offset)
  }

  private def itemOffset(v: String): Int = {
    if (v == null || v == "") 0 else {
      val offsetV = new BigInteger(v, 16)
      val pos = util.Arrays.binarySearch(itemIds.asInstanceOf[Array[AnyRef]], offsetV)
      if (pos < 0) -pos - 1 else pos
    }
  }

  private def tagOffset(v: Tag): Int = {
    if (v == null || v.key == "") 0 else {
      val pos = util.Arrays.binarySearch(tagIndex.asInstanceOf[Array[AnyRef]], v)
      if (pos == -1) 0 else if (pos < -1) -pos - 1 else pos
    }
  }

  def findTags(query: TagQuery): List[Tag] = {
    import com.netflix.atlas.core.model.Query._
    val q = query.query
    val k = query.key
    val offset = tagOffset(query.offsetTag)
    if (q.isDefined || k.isDefined) {
      // If key is restricted add a has query to search
      val finalQ = if (k.isEmpty) q.get else {
        if (q.isDefined) And(HasKey(k.get), q.get) else HasKey(k.get)
      }

      // Count how many tags match the query
      val counts = new IntIntHashMap(-1)
      val itemSet = findImpl(finalQ, 0)
      val iter = itemSet.iterator
      while (iter.hasNext) {
        val tags = itemTags(iter.next())
        var i = 0
        while (i < tags.length) {
          val t = tags(i)
          if (t >= offset && (k.isEmpty || tagIndex(t).key == k.get)) {
            counts.increment(t, 1)
          }
          i += 1
        }
      }

      // Create array with final set of matching tags
      val result = new Array[Tag](counts.size)
      var i = 0
      counts.foreach { (k, v) =>
        val t = tagIndex(k)
        result(i) = Tag(t.key, t.value, v)
        i += 1
      }
      util.Arrays.sort(result.asInstanceOf[Array[AnyRef]])

      // Create list based on limit per page
      val limit = math.min(query.limit, result.length)
      val listBuilder = List.newBuilder[Tag]
      i = 0
      while (i < limit) {
        listBuilder += result(i)
        i += 1
      }
      listBuilder.result
    } else {
      // If no query, use precomputed array of all tags
      val limit = math.min(query.extendedLimit(offset), tagIndex.length)
      val listBuilder = List.newBuilder[Tag]
      var i = offset
      while (i < limit) {
        listBuilder += tagIndex(i)
        i += 1
      }
      listBuilder.result
    }
  }

  def findKeys(query: TagQuery): List[TagKey] = {
    findValues(query).map { v => TagKey(v, -1) }
  }

  def findValues(query: TagQuery): List[String] = {
    val k = query.key
    if (k.isDefined) {
      val offset = k.get + "," + query.offset
      val tags = findTags(TagQuery(query.query, k, offset, query.limit))
      tags.map(_.value)
    } else {
      import scala.collection.JavaConverters._
      val tags = findTags(TagQuery(query.query))
      val dedupedKeys = new java.util.HashSet[String]
      tags.foreach { t =>
        if (t.key > query.offset) dedupedKeys.add(t.key)
      }
      dedupedKeys.asScala.toList.sortWith(_ < _).take(query.limit)
    }
  }

  def findItems(query: TagQuery): List[T] = {
    val offset = itemOffset(query.offset)
    val limit = query.limit
    val list = List.newBuilder[T]
    val intSet = query.query.fold(withOffset(all, offset))(q => findImpl(q, offset))
    val iter = intSet.iterator
    var count = 0
    while (iter.hasNext && count < limit) {
      list += items(iter.next())
      count += 1
    }
    list.result
  }

  val size: Int = items.length
}
