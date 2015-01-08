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
package com.netflix.atlas.core.index

import java.util
import java.util.Comparator

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.Interner
import gnu.trove.map.hash.TIntIntHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import gnu.trove.procedure.TIntIntProcedure
import gnu.trove.procedure.TObjectIntProcedure
import gnu.trove.set.hash.TIntHashSet
import org.slf4j.LoggerFactory


object LazyTagIndex {
  private val logger = LoggerFactory.getLogger(getClass)

  def empty[T <: TaggedItem: Manifest]: LazyTagIndex[T] = {
    new LazyTagIndex(new Array[T](0))
  }
}

class LazyTagIndex[T <: TaggedItem](items: Array[T], interner: Interner[String])
    extends TagIndex[T] {

  import com.netflix.atlas.core.index.LazyTagIndex._

  def this(items: Array[T]) = this(items, Interner.forStrings)

  type LazyValueMap = util.IdentityHashMap[String, LazySet]
  type LazyKeyMap = util.IdentityHashMap[String, LazyValueMap]

  type ValueMap = util.IdentityHashMap[String, TIntHashSet]
  type KeyMap = util.IdentityHashMap[String, ValueMap]

  // Comparator for ordering tagged items using the id
  private val idComparator = new Comparator[T] {
    def compare(t1: T, t2: T): Int = t1.id compareTo t2.id
  }

  // Precomputed set of all items
  private val all = LazySet.all(items.length)

  // Primary indexes to search for a tagged item:
  // * itemIds: sorted array of item ids
  // * itemIndex: key -> value -> set, the set contains indexes to the items array
  // * keyIndex: key -> set, precomputed union of all sets for a given key
  private val (itemIds, itemIndex, keyIndex) = buildItemIndex()

  // Indexes to search for matching tags
  // * tagsIndex: sorted array of all unique tags and overall counts
  // * itemTags: itemTags(i) has an array of indexes into tags array for items(i)
  private val (tagIndex, itemTags) = buildTagIndex()

  private def buildItemIndex(): (Array[String], LazyKeyMap, LazyValueMap) = {
    // Sort items array based on the id, allows for efficient paging of requests using the id
    // as the offset
    logger.debug(s"building index with ${items.length} items, starting sort")
    util.Arrays.sort(items, idComparator)
    val itemIds = new Array[String](items.length)

    // Build the main index
    logger.debug(s"building index with ${items.length} items, create main key map")
    val kidx = new ValueMap
    val idx = new KeyMap
    var pos = 0
    while (pos < items.length) {
      itemIds(pos) = items(pos).idString
      items(pos).foreach { (k, v) =>
        val internedK = interner.intern(k)
        var vidx = idx.get(internedK)
        if (vidx == null) {
          vidx = new ValueMap
          idx.put(internedK, vidx)
        }

        // Add to value index
        val internedV = interner.intern(v)
        var matchSet = vidx.get(internedV)
        if (matchSet == null) {
          matchSet = new TIntHashSet
          vidx.put(internedV, matchSet)
        }
        matchSet.add(pos)

        // Add to key index
        matchSet = kidx.get(internedK)
        if (matchSet == null) {
          matchSet = new TIntHashSet
          kidx.put(internedK, matchSet)
        }
        matchSet.add(pos)
      }
      pos += 1
    }

    // Build final item index
    logger.debug(s"building index with ${items.length} items, create lazy index")
    val lazyIdx = new LazyKeyMap
    val keys = idx.entrySet.iterator
    while (keys.hasNext) {
      val lazyVidx = new LazyValueMap
      val keyEntry = keys.next()
      val values = keyEntry.getValue.entrySet.iterator
      while (values.hasNext) {
        val valueEntry = values.next()
        val lazySet = LazySet(valueEntry.getValue.toArray)
        lazyVidx.put(valueEntry.getKey, lazySet)
      }
      lazyIdx.put(keyEntry.getKey, lazyVidx)
    }

    // Build final key index
    logger.debug(s"building index with ${items.length} items, create key index")
    val lazyKidx = new LazyValueMap
    val keyIter = kidx.entrySet.iterator
    while (keyIter.hasNext) {
      val keyEntry = keyIter.next()
      val lazySet = LazySet(keyEntry.getValue.toArray)
      lazyKidx.put(keyEntry.getKey, lazySet)
    }

    (itemIds, lazyIdx, lazyKidx)
  }

  private def buildTagIndex(): (Array[Tag], Array[Array[Int]]) = {
    // Count how many times each tag occurs
    logger.debug(s"building tag index with ${items.length} items, compute tag counts")
    val tagCounts = new TObjectIntHashMap[(String, String)]
    var pos = 0
    while (pos < items.length) {
      items(pos).foreach { (k, v) =>
        val t = k -> v
        tagCounts.adjustOrPutValue(t, 1, 1)
      }
      pos += 1
    }

    // Create sorted array with tags and the overall counts
    logger.debug(s"building tag index with ${items.length} items, sort and overall counts")
    val tagsArrayBuilder = new TagsArrayBuilder(tagCounts.size)
    tagCounts.forEachEntry(tagsArrayBuilder)
    val tagsArray = tagsArrayBuilder.result
    util.Arrays.sort(tagsArray.asInstanceOf[Array[AnyRef]])

    // Create map of tag to position in tags array
    logger.debug(s"building tag index with ${items.length} items, tag to position map")
    val posMap = new TObjectIntHashMap[(String, String)]
    pos = 0
    while (pos < tagsArray.length) {
      val t = tagsArray(pos)
      posMap.put(t.key -> t.value, pos)
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
        tagsArray(i) = posMap.get(k -> v)
        i += 1
      }
      itemTags(pos) = tagsArray
      pos += 1
    }

    (tagsArray, itemTags)
  }

  private final class TagsArrayBuilder(sz: Int) extends TObjectIntProcedure[(String, String)] {
    val result = new Array[Tag](sz)
    var next = 0

    def execute(k: (String, String), v: Int): Boolean = {
      result(next) = Tag(interner.intern(k._1), interner.intern(k._2), v)
      next += 1
      true
    }
  }

  private final class TagIndexBuilder(sz: Int) extends TObjectIntProcedure[(String, String)] {
    val result = new Array[(String, String)](sz)

    def execute(k: (String, String), v: Int): Boolean = {
      result(v - 1) = interner.intern(k._1) -> interner.intern(k._2)
      true
    }
  }

  private final class TagListBuilder extends TIntIntProcedure {
    val list = List.newBuilder[Tag]

    def execute(k: Int, v: Int): Boolean = {
      val t = tagIndex(k)
      list += Tag(t.key, t.value, v)
      true
    }

    def result: List[Tag] = list.result
  }

  private final class FindTagsBuilder(size: Int) extends TIntIntProcedure {
    val buffer = new Array[Tag](size)
    var next = 0

    def execute(k: Int, v: Int): Boolean = {
      val t = tagIndex(k)
      buffer(next) = Tag(t.key, t.value, v)
      next += 1
      true
    }

    def result: Array[Tag] = buffer
  }

  private[index] def findImpl(query: Query, offset: Int, andSet: Option[LazySet]): LazySet = {
    import com.netflix.atlas.core.model.Query._
    query match {
      case And(q1, q2)            => and(q1, q2, offset, andSet)
      case Or(q1, q2)             => or(q1, q2, offset, andSet)
      case Not(q)                 => all.diff(findImpl(q, offset, None))
      case Equal(k, v)            => equal(k, v, offset)
      case GreaterThan(k, v)      => greaterThan(k, v, false)
      case GreaterThanEqual(k, v) => greaterThan(k, v, true)
      case LessThan(k, v)         => lessThan(k, v, false)
      case LessThanEqual(k, v)    => lessThan(k, v, true)
      case q: In                  => findImpl(q.toOrQuery, offset, andSet)
      case q: PatternQuery        => strPattern(q, offset, andSet)
      case HasKey(k)              => hasKey(k, offset)
      case True                   => all
      case False                  => LazySet.empty
    }
  }

  private def and(q1: Query, q2: Query, offset: Int, andSet: Option[LazySet]): LazySet = {
    val s1 = findImpl(q1, offset, andSet)
    if (s1.isEmpty) LazySet.empty else {
      val s = if (andSet.isDefined) Some(andSet.get.intersect(s1)) else Some(s1)
      val s2 = findImpl(q2, offset, s)
      if (s1.isInstanceOf[BitMaskSet]) {
        s1.asInstanceOf[BitMaskSet].mutableIntersect(s2)
        s1
      } else if (s2.isInstanceOf[BitMaskSet]) {
        s2.asInstanceOf[BitMaskSet].mutableIntersect(s1)
        s2
      } else {
        s1.intersect(s2)
      }
    }
  }

  private def or(q1: Query, q2: Query, offset: Int, andSet: Option[LazySet]): LazySet = {
    val s1 = findImpl(q1, offset, andSet)
    val s2 = findImpl(q2, offset, andSet)
    if (s1.isInstanceOf[BitMaskSet]) {
      s1.asInstanceOf[BitMaskSet].mutableUnion(s2)
      s1
    } else if (s2.isInstanceOf[BitMaskSet]) {
      s2.asInstanceOf[BitMaskSet].mutableUnion(s1)
      s2
    } else {
      s1.union(s2)
    }
  }

  private def equal(k: String, v: String, offset: Int): LazySet = {
    val internedK = interner.intern(k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) LazySet.empty else {
      val internedV = interner.intern(v)
      val matchSet = vidx.get(internedV)
      if (matchSet == null) LazySet.empty else matchSet.offset(offset).workingCopy
    }
  }

  private def greaterThan(k: String, v: String, orEqual: Boolean): LazySet = {
    val internedK = interner.intern(k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) LazySet.empty else {
      val set = LazySet.emptyBitMaskSet
      val prefix = v
      val tag = Tag(internedK, v, -1)
      var i = tagOffset(tag)
      // Skip if equal
      if (!orEqual && i < tagIndex.length && tagIndex(i).key == internedK && tagIndex(i).value == v) {
        i += 1
      }
      // Data is sorted, no need to perform a check for each entry if key matches
      while (i < tagIndex.length && tagIndex(i).key == internedK) {
        set.mutableUnion(vidx.get(tagIndex(i).value))
        i += 1
      }
      set
    }
  }

  private def lessThan(k: String, v: String, orEqual: Boolean): LazySet = {
    val internedK = interner.intern(k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) LazySet.empty else {
      val set = LazySet.emptyBitMaskSet
      val prefix = v
      val tag = Tag(internedK, v, -1)
      var i = tagOffset(tag)
      // Skip if equal
      if (!orEqual && i >= 0 && tagIndex(i).key == internedK && tagIndex(i).value == v) {
        i -= 1
      }
      // Data is sorted, no need to perform a check for each entry if key matches
      while (i >= 0 && tagIndex(i).key == internedK) {
        set.mutableUnion(vidx.get(tagIndex(i).value))
        i -= 1
      }
      set
    }
  }

  private def strPattern(q: Query.PatternQuery, offset: Int, andSet: Option[LazySet]): LazySet = {
    val internedK = interner.intern(q.k)
    val vidx = itemIndex.get(internedK)
    if (vidx == null) LazySet.empty else {
      val set = LazySet.emptyBitMaskSet
      if (q.pattern.prefix.isDefined) {
        val prefix = q.pattern.prefix.get
        val tag = Tag(internedK, prefix, -1)
        var i = tagOffset(tag)
        while (i < tagIndex.length &&
               tagIndex(i).key == internedK &&
               tagIndex(i).value.startsWith(prefix)) {
          if (q.check(tagIndex(i).value)) {
            set.mutableUnion(vidx.get(tagIndex(i).value))
          }
          i += 1
        }
      } else {
        if (andSet.isDefined) {
          val s = andSet.get
          val iter = s.iterator
          while (iter.hasNext) {
            val i = iter.next()
            val item = items(i)
            val value = item.tags.get(internedK)
            if (value.isDefined && q.check(value.get)) {
              set.mask.set(i)
            }
          }
        } else {
          val entries = vidx.entrySet.iterator
          while (entries.hasNext) {
            val entry = entries.next()
            if (q.check(entry.getKey))
              set.mutableUnion(entry.getValue.offset(offset))
          }
        }
      }
      set
    }
  }

  private def hasKey(k: String, offset: Int): LazySet = {
    val internedK = interner.intern(k)
    val matchSet = keyIndex.get(internedK)
    if (matchSet == null) LazySet.empty else matchSet.offset(offset).workingCopy
  }

  private def itemOffset(v: String): Int = {
    if (v == null || v == "") 0 else {
      val pos = util.Arrays.binarySearch(itemIds.asInstanceOf[Array[AnyRef]], v)
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
      val finalQ = if (!k.isDefined) q.get else {
        if (q.isDefined) And(HasKey(k.get), q.get) else HasKey(k.get)
      }

      // Count how many tags match the query
      val counts = new TIntIntHashMap
      val itemSet = findImpl(finalQ, 0, None)
      val iter = itemSet.iterator
      while (iter.hasNext) {
        val tags = itemTags(iter.next())
        var i = 0
        while (i < tags.length) {
          val t = tags(i)
          if (t >= offset && (!k.isDefined || tagIndex(t).key == k.get)) {
            counts.adjustOrPutValue(t, 1, 1)
          }
          i += 1
        }
      }

      // Create array with final set of matching tags
      val builder = new FindTagsBuilder(counts.size)
      counts.forEachEntry(builder)
      val result = builder.result
      util.Arrays.sort(result.asInstanceOf[Array[AnyRef]])

      // Create list based on limit per page
      val limit = math.min(query.limit, result.length)
      val listBuilder = List.newBuilder[Tag]
      var i = 0
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
      import scala.collection.JavaConversions._
      val tags = findTags(TagQuery(query.query))
      val dedupedKeys = new java.util.HashSet[String]
      tags.foreach { t =>
        if (t.key > query.offset) dedupedKeys.add(t.key)
      }
      dedupedKeys.toList.sortWith(_ < _).take(query.limit)
    }
  }

  def findItems(query: TagQuery): List[T] = {
    val offset = itemOffset(query.offset)
    val limit = query.limit
    val list = List.newBuilder[T]
    val intSet = query.query.fold(all.offset(offset))(q => findImpl(q, offset, None))
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
