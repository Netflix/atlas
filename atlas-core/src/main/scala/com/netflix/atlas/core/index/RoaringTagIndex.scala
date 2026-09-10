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
package com.netflix.atlas.core.index

import java.util
import java.util.Comparator
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.IntRefHashMap
import com.netflix.atlas.core.util.LongHashSet
import com.netflix.atlas.core.util.RefIntHashMap
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * Create a new index based on roaring bitmaps.
  *
  * https://github.com/RoaringBitmap/RoaringBitmap
  *
  * @param items
  *     Items to include in the index. The array must already be sorted by id and should not
  *     be used or modified outside of the index.
  * @param stats
  *     Used to track stats related to the index.
  */
class RoaringTagIndex[T <: TaggedItem](items: Array[T], stats: IndexStats) extends TagIndex[T] {

  import com.netflix.atlas.core.index.RoaringTagIndex.*

  type RoaringValueMap = IntRefHashMap[RoaringBitmap]
  type RoaringKeyMap = IntRefHashMap[RoaringValueMap]

  // Precomputed set of all items
  private val all = {
    val set = new RoaringBitmap()
    set.add(0L, items.length)
    set
  }

  private val (keys, values) = {
    // Initial capacities are based on observed production data where the number of
    // unique keys is typically under 10k and unique values are roughly 1/6th of the
    // number of items. These are much smaller than items.length, avoiding large
    // temporary allocations during construction.
    val keySet = new util.HashSet[String](1024)
    val valueSet = new util.HashSet[String](math.max(1024, items.length / 4))
    // Hoisted once rather than allocating a fresh closure per item; it captures only the
    // two stable sets, so a single instance is reused across all items.
    val collect: (String, String) => Unit = (k, v) => {
      keySet.add(k)
      valueSet.add(v)
    }
    var pos = 0
    while (pos < items.length) {
      items(pos).foreach(collect)
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
  // * itemTagOffsets/itemTagData: flat representation of key value pairs for each item.
  //   itemTagData is a single array of [k1,v1,k2,v2,...] pairs for all items concatenated.
  //   itemTagOffsets maps item index to the start position in itemTagData. The key and
  //   value numbers are positions in the keys and values arrays respectively.
  private val (itemIds, itemIndex, keyIndex, tagIndex, itemTagOffsets, itemTagData) =
    buildItemIndex()

  // Optimize all bitmaps for memory after the index is fully built. Items are inserted
  // sequentially so many bitmaps will have consecutive runs that compress well.
  optimizeBitmaps()

  // Collect and log various index stats
  collectStats()

  private def optimizeBitmaps(): Unit = {
    keyIndex.foreach { (_, bitmap) =>
      bitmap.runOptimize()
      bitmap.trim()
    }
    itemIndex.foreach { (_, vidx) =>
      vidx.foreach { (_, bitmap) =>
        bitmap.runOptimize()
        bitmap.trim()
      }
    }
  }

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
    : (Array[ItemId], RoaringKeyMap, RoaringValueMap, Array[Long], Array[Int], Array[Int]) = {

    // Sort items array based on the id, allows for efficient paging of requests using the id
    // as the offset
    logger.debug(s"building index with ${items.length} items, starting sort")
    val itemIds = new Array[ItemId](items.length)

    // Compute offsets for flat itemTagData array
    val tagOffsets = new Array[Int](items.length + 1)
    var totalTags = 0
    var i = 0
    while (i < items.length) {
      tagOffsets(i) = totalTags
      totalTags += 2 * items(i).tags.size
      i += 1
    }
    tagOffsets(items.length) = totalTags
    val tagData = new Array[Int](totalTags)

    // Build the main index
    logger.debug(s"building index with ${items.length} items, create main key map")
    val kidx = new RoaringValueMap(-1)
    val idx = new RoaringKeyMap(-1)
    val tagsSet = new LongHashSet(-1L, items.length)

    // Single reusable consumer for the per-item tag iteration. A `(k, v) => ...` lambda
    // here would allocate a fresh closure for every item (plus an `IntRef` to box the
    // mutable `itemTagsPos` it captures); with ~millions of items per rebuild that is a
    // top allocation leaf. The stable maps are captured once and the per-item `pos` /
    // `itemTagsPos` are set as fields before each item is iterated.
    final class TagConsumer extends ((String, String) => Unit) {
      var pos: Int = 0
      var itemTagsPos: Int = 0

      def apply(k: String, v: String): Unit = {
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

        tagData(itemTagsPos) = kp
        tagData(itemTagsPos + 1) = vp
        itemTagsPos += 2

        val t = (kp.toLong << 32) | vp.toLong
        tagsSet.add(t)
      }
    }

    val consumer = new TagConsumer
    var pos = 0
    while (pos < items.length) {
      itemIds(pos) = items(pos).id
      consumer.pos = pos
      consumer.itemTagsPos = tagOffsets(pos)
      items(pos).foreach(consumer)
      pos += 1
    }

    val tagsArray = tagsSet.toArray
    util.Arrays.sort(tagsArray)

    (itemIds, idx, kidx, tagsArray, tagOffsets, tagData)
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

  /**
    * Evaluate a query into the set of matching item positions.
    *
    * OWNERSHIP: the returned bitmap must be treated as READ-ONLY. For some queries it
    * is a shared reference into the index (e.g. the stored bitmap for an exact tag
    * match, or `all`), and mutating it would corrupt the index for every later query.
    * The combinators (`and`, `or`, `andNot`) never mutate a bitmap that could be shared
    * with the index, so they can consume this directly. Their own result is always
    * freshly allocated, which is what lets `or` fold a chain into the accumulator
    * returned by a nested `or`. Callers that need to mutate the result (offset removal
    * in [[findItems]]) must obtain a copy via [[findOwned]].
    *
    * Offset is NOT applied here. Removing a fixed prefix range commutes with
    * and/or/andNot, so it is applied once on the final set at the top-level entry
    * points instead of being cloned into every leaf.
    */
  private def findReadOnly(query: Query): RoaringBitmap = {
    import com.netflix.atlas.core.model.Query.*
    query match {
      case And(q1, q2)            => and(q1, q2)
      case Or(q1, q2)             => or(q1, q2)
      case Not(q)                 => diff(all, findReadOnly(q))
      case Equal(k, v)            => equal(k, v)
      case GreaterThan(k, v)      => greaterThan(k, v, false)
      case GreaterThanEqual(k, v) => greaterThan(k, v, true)
      case LessThan(k, v)         => lessThan(k, v, false)
      case LessThanEqual(k, v)    => lessThan(k, v, true)
      case In(k, vs)              => in(k, vs)
      case q: PatternQuery        => strPattern(q)
      case HasKey(k)              => hasKey(k)
      case True                   => all
      case False                  => new RoaringBitmap()
    }
  }

  /**
    * Evaluate a query into a bitmap the caller OWNS and may mutate in place. Only the
    * cases that [[findReadOnly]] can answer with a shared index bitmap need a copy;
    * every other case already produces a freshly-allocated set.
    *
    * MAINTENANCE: the shared-returning leaves are `Equal` (`vidx.get`), `HasKey`
    * (`keyIndex.get`), `True` (`all`), and `In`/`PatternQuery` when exactly one value
    * matches (see `UnionAccumulator`). If a new leaf is added that returns a stored
    * index bitmap rather than a fresh one, it MUST be added to the match below, or
    * mutating the result will corrupt the index. (See the "ownership" tests in
    * RoaringTagIndexSuite.)
    */
  private def findOwned(query: Query): RoaringBitmap = {
    import com.netflix.atlas.core.model.Query.*
    query match {
      case _: Equal | _: HasKey | _: In | _: PatternQuery | True =>
        ownedCopy(findReadOnly(query))
      case _ =>
        findReadOnly(query)
    }
  }

  /** Copy a possibly-shared bitmap so it can be mutated. Empty sets are already owned. */
  private def ownedCopy(set: RoaringBitmap): RoaringBitmap = {
    if (set.isEmpty) set else set.clone()
  }

  /**
    * Size of the accumulated set below which a union-building term (`In`, pattern) is
    * evaluated by walking the candidate items instead of materializing the union.
    *
    * Set from a sweep of `acc` sizes across all four term types. At ~5k candidates the walk
    * uses 2-6x fewer bytes than the union and is no slower for a range, a pattern or a
    * multi-value `In`; a two value `In` is the one shape that pays, 65us against 39us, because
    * its union is cheap to build. By 20k the walk is 2-4x slower across the board and by 150k
    * it loses on bytes too, since it is O(|acc|) with a tag lookup per item.
    *
    * The crossover is genuinely per term type -- the union cost scales with how many values it
    * merges, the walk cost with |acc| -- so a range tolerates a far higher threshold than a two
    * value `In`. One threshold for all of them is deliberate, and it has a known cost: on a key
    * with very few values the union has almost nothing to merge, so the walk saves bytes but
    * loses time. Measured at ~2.5k candidates:
    *
    * {{{
    * key          values   walk                 union
    * statistic         3   47,016 B /  64 us    61,664 B /  22 us
    * nf.region         4  100,744 B /  68 us   219,872 B /  50 us
    * name             20   75,304 B /  74 us   219,094 B / 269 us
    * }}}
    *
    * The walk always allocates less, which is what this path exists to fix, but is up to 3x
    * slower once a key has three or four values. Gating on the key's value count would recover
    * the `statistic` row and lose the `nf.region` one -- 119k of allocation to save 17us -- so
    * the crossover is a single value wide and not worth a second threshold to chase.
    */
  protected def itemScanThreshold: Int = 8192

  private def diff(s1: RoaringBitmap, s2: RoaringBitmap): RoaringBitmap = {
    // Static andNot allocates a new owned result and reads (does not mutate) s1/s2,
    // so passing the shared `all` for s1 is safe.
    RoaringBitmap.andNot(s1, s2)
  }

  private def withOffset(set: RoaringBitmap, offset: Int): RoaringBitmap = {
    if (offset > 0)
      set.remove(0L, offset + 1L)
    set
  }

  /**
    * Cost of evaluating a conjunct on its own, used to order the terms of an `AND` chain.
    * The cheap terms are the ones that can be answered with a bitmap already stored in the
    * index; everything else has to build a union or a complement.
    */
  private def conjunctCost(query: Query): Long = {
    import com.netflix.atlas.core.model.Query.*
    // The cheap terms are answered with a bitmap already stored in the index. Order those
    // by how many items they match so the accumulated set starts as small as possible;
    // everything downstream is bounded by it. Cardinality is a sum over the containers of
    // an already built bitmap, so this is cheap relative to evaluating the term.
    //
    // The tiers are spaced by 2^32 and the cardinality is an `Int`, so a within-tier
    // ordering can never spill into the next tier.
    def tier(t: Int): Long = t.toLong << 32
    query match {
      case False                       => tier(0)
      case _: Equal | _: HasKey | True => tier(1) + findReadOnly(query).getCardinality
      case _: In                       => tier(2)
      case _: PatternQuery             => tier(3)
      case _: GreaterThan | _: GreaterThanEqual | _: LessThan | _: LessThanEqual => tier(4)
      case _: Or                                                                 => tier(5)
      // A negation is applied as an `andNot` against the accumulated set, so it never
      // materializes a complement as long as something cheaper goes first.
      case _: Not => tier(6)
      case _      => tier(5)
    }
  }

  /** Number of leaves in an `AND` chain, used to size the array for [[flatten]] exactly. */
  private def countConjuncts(query: Query): Int = {
    query match {
      case Query.And(q1, q2) => countConjuncts(q1) + countConjuncts(q2)
      case _                 => 1
    }
  }

  /**
    * Write the leaves of an `AND` chain into `dest` starting at `pos`, returning the next
    * free slot.
    */
  private def flatten(query: Query, dest: Array[Query], pos: Int): Int = {
    query match {
      case Query.And(q1, q2) => flatten(q2, dest, flatten(q1, dest, pos))
      case q                 =>
        dest(pos) = q
        pos + 1
    }
  }

  /**
    * Order the conjuncts in place, cheapest first. Each term's cost is computed once up
    * front rather than re-derived per comparison, since `conjunctCost` evaluates the cheap
    * leaves to get their cardinality.
    *
    * An insertion sort over a pair of arrays keeps the whole plan down to two allocations
    * for the two to five term chains that dominate. `sortBy` over a decorated list costs a
    * tuple and a boxed `Long` per term plus the intermediate array and rebuilt lists, which
    * is a lot of garbage to add to the very path this change exists to make quieter. It is
    * also stable, so terms within a tier keep the order they were written in.
    */
  private def sortByCost(conjuncts: Array[Query]): Unit = {
    val n = conjuncts.length
    val costs = new Array[Long](n)
    var i = 0
    while (i < n) {
      costs(i) = conjunctCost(conjuncts(i))
      i += 1
    }
    i = 1
    while (i < n) {
      val cost = costs(i)
      val q = conjuncts(i)
      var j = i - 1
      while (j >= 0 && costs(j) > cost) {
        costs(j + 1) = costs(j)
        conjuncts(j + 1) = conjuncts(j)
        j -= 1
      }
      costs(j + 1) = cost
      conjuncts(j + 1) = q
      i += 1
    }
  }

  /**
    * Evaluate an `AND` chain. The conjuncts are flattened and evaluated cheapest first so
    * the union-building terms (`In`, patterns, ranges) run against an already narrowed set
    * rather than against the whole index.
    *
    * Ordering alone only buys the empty short circuit. The win is that once `acc` has been
    * narrowed, a union-building term can be answered by walking the candidates rather than
    * materializing the union -- which for a query such as `nf.app=x AND name IN (a, b)`
    * means never promoting the `name` sets into 8k bitmap containers that the surrounding
    * `AND` immediately discards.
    */
  private def and(q1: Query, q2: Query): RoaringBitmap = {
    val n = countConjuncts(q1) + countConjuncts(q2)
    val conjuncts = new Array[Query](n)
    flatten(q2, conjuncts, flatten(q1, conjuncts, 0))
    sortByCost(conjuncts)

    // There are always at least two conjuncts, so when the first is non-empty the loop
    // runs at least once and `acc` is replaced by an owned set. The empty check at the end
    // covers the other path, where `acc` can still be a shared index bitmap (`all` on an
    // empty index).
    var acc = findReadOnly(conjuncts(0))
    var i = 1
    while (i < n && !acc.isEmpty) {
      acc = intersect(acc, conjuncts(i))
      i += 1
    }
    if (acc.isEmpty) new RoaringBitmap() else acc
  }

  /**
    * Intersect an already computed set with one more conjunct. The result is always a
    * freshly allocated set, so the caller may treat it as owned even though `acc` itself
    * may be a shared bitmap from the index on the first call.
    */
  private def intersect(acc: RoaringBitmap, query: Query): RoaringBitmap = {
    import com.netflix.atlas.core.model.Query.*
    query match {
      // `acc AND NOT(q)` is `acc \ q`. Since acc is a subset of `all`, this can go
      // directly against the sub-query and never materializes the complement.
      case Not(q)                                   => diff(acc, findReadOnly(q))
      case In(k, vs) if walkCandidates(acc)         => andIn(acc, k, vs)
      case q: PatternQuery if walkCandidates(acc)   => andPattern(acc, q)
      case GreaterThan(k, v) if walkCandidates(acc) =>
        andRange(acc, k, findOffset(values, v, 1), Int.MaxValue)
      case GreaterThanEqual(k, v) if walkCandidates(acc) =>
        andRange(acc, k, findOffset(values, v, 0), Int.MaxValue)
      case LessThan(k, v) if walkCandidates(acc) =>
        andRange(acc, k, 0, findOffsetLessThan(values, v, -1))
      case LessThanEqual(k, v) if walkCandidates(acc) =>
        andRange(acc, k, 0, findOffsetLessThan(values, v, 0))
      // Everything else, including a union-building term once `acc` is too large to walk:
      // `findReadOnly` materializes the union and this intersects it in one pass.
      case q => RoaringBitmap.and(acc, findReadOnly(q))
    }
  }

  private def walkCandidates(acc: RoaringBitmap): Boolean = {
    // Cardinality is a sum of a stored count per container, so this allocates nothing and
    // is proportional to the container count rather than the item count.
    acc.getCardinality <= itemScanThreshold
  }

  /**
    * `acc AND In(k, vs)`, evaluated by walking the candidate items. Only called once `acc` is
    * small enough (see [[walkCandidates]]), so this is O(|acc|) and allocates just the result --
    * never the union, which the surrounding `AND` would discard nearly all of.
    *
    * MAINTENANCE: this loop is repeated in [[andPattern]] and [[andRange]] rather than shared
    * behind a predicate. A common `walk(acc, kp, Int => Boolean)` helper is tidier, but the
    * per-candidate call site goes megamorphic as soon as a process evaluates more than one term
    * type, and measured up to 10x slower on this path once it does.
    */
  private def andIn(acc: RoaringBitmap, k: String, vs: List[String]): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    if (itemIndex.get(kp) == null) new RoaringBitmap()
    else {
      // Bounded by the query rather than by the data, so materializing it is safe.
      val vps = new Array[Int](vs.size)
      var n = 0
      vs.foreach { v =>
        val vp = valueMap.get(v, -1)
        if (vp >= 0) {
          vps(n) = vp
          n += 1
        }
      }
      util.Arrays.sort(vps, 0, n)
      val result = new RoaringBitmap()
      val iter = acc.getIntIterator
      while (iter.hasNext) {
        val pos = iter.next()
        val v = getValue(pos, kp)
        // Positions come out in ascending order, the cheap case for roaring inserts.
        if (v >= 0 && util.Arrays.binarySearch(vps, 0, n, v) >= 0)
          result.add(pos)
      }
      result
    }
  }

  /**
    * `acc AND q` for a pattern query, evaluated by walking the candidate items. The set of
    * matching values is bounded by the data, not the query -- a loose pattern on a high
    * cardinality key matches hundreds of thousands of values -- so it is never collected; the
    * pattern is applied to each candidate's value instead. See [[andIn]] on why the loop is
    * repeated rather than shared.
    */
  private def andPattern(acc: RoaringBitmap, q: Query.PatternQuery): RoaringBitmap = {
    val kp = keyMap.get(q.k, -1)
    if (itemIndex.get(kp) == null) new RoaringBitmap()
    else {
      val result = new RoaringBitmap()
      val iter = acc.getIntIterator
      while (iter.hasNext) {
        val pos = iter.next()
        val v = getValue(pos, kp)
        if (v >= 0 && q.check(values(v)))
          result.add(pos)
      }
      result
    }
  }

  /**
    * `acc AND` a range term, evaluated by walking the candidate items. `lo` and `hi` are an
    * inclusive range of positions in [[values]].
    *
    * The range collapses to an integer comparison because `values` is sorted, so a value's
    * position orders the same way the value itself does. That makes this the cheapest of the
    * three walks, and it avoids the union over every value in the range that [[greaterThan]] and
    * [[lessThan]] would otherwise build -- the widest union of the term types, since a range can
    * cover most of the values for a key. See [[andIn]] on why the loop is repeated.
    */
  private def andRange(acc: RoaringBitmap, k: String, lo: Int, hi: Int): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    // `hi` is -1 when the bound sorts below every known value, which matches nothing.
    if (itemIndex.get(kp) == null || hi < lo) new RoaringBitmap()
    else {
      val result = new RoaringBitmap()
      val iter = acc.getIntIterator
      while (iter.hasNext) {
        val pos = iter.next()
        // lo is never negative, so this also rejects the -1 for an item without the key.
        val v = getValue(pos, kp)
        if (v >= lo && v <= hi)
          result.add(pos)
      }
      result
    }
  }

  private def or(q1: Query, q2: Query): RoaringBitmap = {
    import com.netflix.atlas.core.model.Query.*
    // A nested `Or` has already allocated a fresh accumulator (see below), so the union
    // can be folded into it. Without this, a chain such as `Or(Or(Or(a, b), c), d)` --
    // the shape produced by expanding `:in` and by query normalization -- would copy the
    // growing result at every level, making the chain quadratic in the result size.
    q1 match {
      case _: Or =>
        val s1 = findReadOnly(q1)
        s1.or(findReadOnly(q2))
        s1
      case _ =>
        q2 match {
          case _: Or =>
            val s2 = findReadOnly(q2)
            s2.or(findReadOnly(q1))
            s2
          case _ =>
            // Static `or` reads both operands, so a shared bitmap from a leaf is fine.
            RoaringBitmap.or(findReadOnly(q1), findReadOnly(q2))
        }
    }
  }

  private def equal(k: String, v: String): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val vp = valueMap.get(v, -1)
      // Shared reference into the index -- read only (see findReadOnly/findOwned).
      val matchSet = vidx.get(vp)
      if (matchSet == null) new RoaringBitmap() else matchSet
    }
  }

  private def greaterThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      // Fresh accumulator -- owned, safe to return for either read-only or owned use.
      val set = new LazyOrBitmap()
      val vp = findOffset(values, v, if (orEqual) 0 else 1)
      val t = tag(kp, vp)
      var i = tagOffset(t)

      // Data is sorted, no need to perform a check for each entry if key matches
      while (i < tagIndex.length && tagKey(tagIndex(i)) == kp) {
        set.naivelazyor(vidx.get(tagValue(tagIndex(i))))
        i += 1
      }
      set.repairAfterLazy()
      set
    }
  }

  private def lessThan(k: String, v: String, orEqual: Boolean): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new LazyOrBitmap()
      val vp = findOffsetLessThan(values, v, if (orEqual) 0 else -1)
      if (vp >= 0) {
        val t = tag(kp, vp)
        var i = tagOffset(t)

        // tagOffset returns the insertion point when the tag is not found, which
        // is the position of the first element greater than t. For backward iteration,
        // adjust to the last element <= t.
        if (i >= tagIndex.length || tagIndex(i) > t) {
          i -= 1
        }

        // Data is sorted, no need to perform a check for each entry if key matches
        while (i >= 0 && tagKey(tagIndex(i)) == kp) {
          set.naivelazyor(vidx.get(tagValue(tagIndex(i))))
          i -= 1
        }
      }
      set.repairAfterLazy()
      set
    }
  }

  private def in(k: String, vs: List[String]): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new UnionAccumulator
      vs.foreach { v =>
        val vp = valueMap.get(v, -1)
        set.add(vidx.get(vp))
      }
      set.result()
    }
  }

  private def strPattern(q: Query.PatternQuery): RoaringBitmap = {
    val kp = keyMap.get(q.k, -1)
    val vidx = itemIndex.get(kp)
    if (vidx == null) new RoaringBitmap()
    else {
      val set = new UnionAccumulator
      val prefix = q.pattern.prefix()
      if (prefix != null) {
        val vp = findOffset(values, prefix, 0)
        val t = tag(kp, vp)
        var i = tagOffset(t)
        while (
          i < tagIndex.length
          && tagKey(tagIndex(i)) == kp
          && values(tagValue(tagIndex(i))).startsWith(prefix)
        ) {
          val v = tagValue(tagIndex(i))
          if (q.check(values(v))) {
            set.add(vidx.get(v))
          }
          i += 1
        }
      } else {
        vidx.foreach { (v, items) =>
          if (q.check(values(v)))
            set.add(items)
        }
      }
      set.result()
    }
  }

  private def hasKey(k: String): RoaringBitmap = {
    val kp = keyMap.get(k, -1)
    // Shared reference into the index -- read only (see findReadOnly/findOwned).
    val matchSet = keyIndex.get(kp)
    if (matchSet == null) new RoaringBitmap() else matchSet
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
    * equal values in the case of strictly greater than comparisons. By default a
    * greater than, `n = 1`, comparison will be done.
    */
  private def findOffset(vs: Array[String], v: String, n: Int = 1): Int = {
    if (v == null || v == "") 0
    else {
      val pos = util.Arrays.binarySearch(vs.asInstanceOf[Array[AnyRef]], v)
      if (pos >= 0) pos + n else -pos - 1
    }
  }

  /**
    * Find the offset for `v` in the array `vs`. If an exact match is found, then
    * the value `n` will be added to the position. This is mostly used for skipping
    * equal values in the case of strictly less than. If no match is found, then it
    * will be the position where the next item less than the value should be.
    */
  private def findOffsetLessThan(vs: Array[String], v: String, n: Int): Int = {
    if (v == null || v == "") 0
    else {
      // Binary search gives position of item if not found, need to skip one position
      // for the position of the item less than.
      val pos = util.Arrays.binarySearch(vs.asInstanceOf[Array[AnyRef]], v)
      if (pos >= 0) pos + n else -pos - 2
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
      val itemSet = findReadOnly(q) // read-only below (iterate); no offset on item ids
      val offset = findOffset(keys, query.offset)

      val results = new util.BitSet(keys.length)
      val iter = itemSet.getIntIterator
      while (iter.hasNext) {
        val itemPos = iter.next()
        var i = itemTagOffsets(itemPos)
        val end = itemTagOffsets(itemPos + 1)
        while (i < end) {
          val k = itemTagData(i)
          if (k >= offset) results.set(k)
          i += 2
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

    val vidx = itemIndex.get(kp)
    if (vidx == null) return Nil

    val offset = findOffset(values, query.offset)
    val results = new util.BitSet(values.length)

    if (query.query.isEmpty) {
      // No query filter, just use all values for a key
      vidx.foreachKey { v =>
        if (v >= offset) {
          results.set(v)
        }
      }
    } else {
      // Need to restrict by the query, always include restriction for items with the key
      val has = Query.HasKey(k)
      val q = query.query.fold[Query](has)(q => q.and(has))
      val itemSet = findReadOnly(q) // read-only below (intersect/iterate)

      // Double check if there were any matches
      if (itemSet.isEmpty) return Nil

      if (vidx.size < itemSet.getCardinality / 8) {
        // Loop over the values for a key since it is considerably smaller than the
        // overall set of matches. All we need to confirm is that there is at least
        // one item matching the query criteria with a given value
        vidx.foreach { (v, items) =>
          if (v >= offset && hasNonEmptyIntersection(itemSet, items)) {
            results.set(v)
          }
        }
      } else {
        // Loop over the items that match the query
        val iter = itemSet.getIntIterator
        while (iter.hasNext) {
          val itemPos = iter.next()
          val v = getValue(itemPos, kp)
          if (v >= offset) {
            results.set(v)
          }
        }
      }
    }

    createResultList(values, results, query.limit)
  }

  private def getValue(itemPos: Int, k: Int): Int = {
    var i = itemTagOffsets(itemPos)
    val end = itemTagOffsets(itemPos + 1)
    while (i < end) {
      if (k == itemTagData(i)) return itemTagData(i + 1)
      i += 2
    }
    -1
  }

  def findItems(query: TagQuery): List[T] = {
    val offset = itemOffset(query.offset)
    val limit = query.limit
    // Apply the offset once on the final set. When offset > 0 the prefix removal
    // mutates, so the set must be owned; otherwise the result is only read by
    // createResultList and a shared bitmap is fine.
    val intSet =
      if (offset > 0)
        withOffset(query.query.fold(all.clone())(q => findOwned(q)), offset)
      else
        query.query.fold(all)(q => findReadOnly(q))
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

  override def iterator: Iterator[T] = items.iterator

  val size: Int = items.length
}

object RoaringTagIndex {

  /** Comparator for ordering tagged items using the id. */
  val IdComparator: Comparator[TaggedItem] = { (t1: TaggedItem, t2: TaggedItem) =>
    t1.id.compareTo(t2.id)
  }

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Create a new tag index.
    *
    * @param items
    *     Items to index. The array will be sorted and should not be used or modified
    *     outside of the index.
    * @param stats
    *     Used to track stats related to the index.
    */
  def apply[T <: TaggedItem](items: Array[T], stats: IndexStats): RoaringTagIndex[T] = {
    util.Arrays.sort(items, IdComparator)
    new RoaringTagIndex[T](items, stats)
  }

  def empty[T <: TaggedItem: ClassTag]: RoaringTagIndex[T] = {
    new RoaringTagIndex(new Array[T](0), new IndexStats())
  }

  private[index] def hasNonEmptyIntersection(b1: RoaringBitmap, b2: RoaringBitmap): Boolean = {
    RoaringBitmap.intersects(b1, b2)
  }

  /**
    * Accumulator for the union of a set of bitmaps from the index. The lazy or
    * accumulator is only created once there are at least two sets to combine. For
    * the common cases of zero or one match that avoids promoting the containers to
    * bitmap containers and repairing them afterwards.
    *
    * Note that with a single match the shared bitmap from the index is returned by
    * [[result]], so it must be treated as read-only (see `findOwned`).
    */
  private[index] class UnionAccumulator {

    private var first: RoaringBitmap = _
    private var acc: LazyOrBitmap = _

    def add(set: RoaringBitmap): Unit = {
      if (set != null) {
        if (acc != null) {
          acc.naivelazyor(set)
        } else if (first == null) {
          first = set
        } else {
          acc = new LazyOrBitmap()
          acc.naivelazyor(first)
          acc.naivelazyor(set)
          first = null
        }
      }
    }

    def result(): RoaringBitmap = {
      if (acc != null) {
        acc.repairAfterLazy()
        acc
      } else if (first != null) {
        first
      } else {
        new RoaringBitmap()
      }
    }
  }
}
