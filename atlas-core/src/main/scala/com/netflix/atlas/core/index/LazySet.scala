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

object LazySet {

  final val END = Integer.MAX_VALUE

  final val empty: LazySet = new EmptySet

  def emptyBitMaskSet: BitMaskSet = new BitMaskSet(new util.BitSet)

  def apply(vs: Int*): LazySet = {
    apply(vs.toArray)
  }

  def apply(vs: Array[Int]): LazySet = {
    util.Arrays.sort(vs)
    newEvaluatedSet(vs, vs.length)
  }

  def all(n: Int): LazySet = {
    val mask = new util.BitSet(n)
    mask.flip(0, n)
    new BitMaskSet(mask)
  }

  private def newEvaluatedSet(vs: Array[Int], length: Int): LazySet = {
    val lastValue = vs(length - 1)
    if (lastValue / length <= 16) {
      val mask = new util.BitSet(lastValue + 1)
      var i = 0
      while (i < length) {
        mask.set(vs(i))
        i += 1
      }
      new BitMaskSet(mask)
    } else {
      new SortedArraySet(vs, length)
    }
  }
}

/**
 * A set of integers that lazily evaluates operations such as union and intersection. The default
 * mode of operation is to return a view that will evaluate the operations when you iterate over
 * the set. This can be beneficial in circumstances where future operations may limit the amount
 * of work that needs to be done, e.g. intersecting with a smaller set.
 */
trait LazySet {
  def contains(k: Int): Boolean
  def offset(k: Int): LazySet
  def diff(s: LazySet): LazySet
  def intersect(s: LazySet): LazySet
  def union(s: LazySet): LazySet
  def iterator: SetIterator

  def isEmpty: Boolean = (maxSize == 0)

  def maxValue: Int

  def depth: Int
  def maxSize: Int

  def toSortedArraySet: LazySet = {
    val size = maxSize
    if (size == 0) {
      LazySet.empty
    } else {
      if (maxSize / maxValue < 16) {
        val mask = new util.BitSet
        val i = iterator
        while (i.hasNext) {
          mask.set(i.next())
        }
        new BitMaskSet(mask)
      } else {
        val buf = new Array[Int](size)
        var pos = 0
        val i = iterator
        while (i.hasNext) {
          buf(pos) = i.next()
          pos += 1
        }
        new SortedArraySet(buf, pos)
      }
    }
  }

  def toSet: Set[Int] = {
    val s = Set.newBuilder[Int]
    val i = iterator
    while (i.hasNext) s += i.next()
    s.result
  }

  def toList: List[Int] = {
    val s = List.newBuilder[Int]
    val i = iterator
    while (i.hasNext) s += i.next()
    s.result
  }

  def workingCopy: LazySet = {
    this
  }
}

abstract class AbstractLazySet extends LazySet {

  protected def shouldEvaluate(s: LazySet): Boolean = {
    depth > 10
  }

  protected def eval(s: LazySet): LazySet = {
    if (shouldEvaluate(s)) s.toSortedArraySet else s
  }

  def offset(k: Int): LazySet = {
    if (k > 0) eval(new OffsetSet(this, k)) else this
  }

  def diff(s: LazySet): LazySet = {
    eval(new DiffSet(this, s))
  }

  def intersect(s: LazySet): LazySet = {
    eval(new IntersectSet(this, s))
  }

  def union(s: LazySet): LazySet = {
    eval(new UnionSet(this, s))
  }
}

trait SetIterator {
  def hasNext: Boolean
  def next(): Int
  def skipTo(k: Int): Boolean
  def maxSize: Int
}

class EmptySet extends LazySet {
  def contains(k: Int): Boolean = false

  def offset(k: Int): LazySet = this

  def diff(s: LazySet): LazySet = this

  def intersect(s: LazySet): LazySet = this

  def union(s: LazySet): LazySet = s

  val iterator: SetIterator = new EmptySetIterator

  val depth: Int = 0

  val maxSize: Int = 0

  val maxValue: Int = LazySet.END

  override lazy val toSet: Set[Int] = Set.empty
}

class EmptySetIterator extends SetIterator {
  val hasNext: Boolean = false
  def next(): Int = LazySet.END
  def skipTo(k: Int): Boolean = false
  val maxSize: Int = 0
}

/**
 * Set based on a bit set as the backing store.
 */
class BitMaskSet(val mask: util.BitSet) extends AbstractLazySet {

  def mutableDiff(s: LazySet) {
    s match {
      case set: BitMaskSet =>
        mask.andNot(set.mask)
      case _ =>
        val i = s.iterator
        while (i.hasNext) mask.clear(i.next())
    }
  }

  def mutableIntersect(s: LazySet) {
    s match {
      case set: BitMaskSet =>
        mask.and(set.mask)
      case _ =>
        val tmp = new util.BitSet
        val i = s.iterator
        while (i.hasNext) tmp.set(i.next())
        mask.and(tmp)
    }
  }

  def mutableUnion(s: LazySet) {
    s match {
      case set: BitMaskSet =>
        mask.or(set.mask)
      case _ =>
        val i = s.iterator
        while (i.hasNext) mask.set(i.next())
    }
  }

  override def diff(s: LazySet): LazySet = {
    s match {
      case set: BitMaskSet =>
        val tmp = new util.BitSet(mask.size)
        tmp.or(mask)
        tmp.andNot(set.mask)
        new BitMaskSet(tmp)
      case _ =>
        super.diff(s)
    }
  }

  override def intersect(s: LazySet): LazySet = {
    s match {
      case set: BitMaskSet =>
        val tmp = new util.BitSet(mask.size)
        tmp.or(mask)
        tmp.and(set.mask)
        new BitMaskSet(tmp)
      case _ =>
        super.intersect(s)
    }
  }

  override def union(s: LazySet): LazySet = {
    s match {
      case set: BitMaskSet =>
        val tmp = new util.BitSet(mask.size)
        tmp.or(mask)
        tmp.or(set.mask)
        new BitMaskSet(tmp)
      case _ =>
        super.union(s)
    }
  }

  def contains(k: Int): Boolean = {
    (k >= 0 && k < mask.size) && mask.get(k)
  }

  def iterator: SetIterator = {
    new BitMaskSetIterator(mask)
  }

  lazy val depth: Int = 0

  def maxSize: Int = mask.size

  def maxValue: Int = mask.previousSetBit(mask.size)

  override def workingCopy: LazySet = {
    val tmp = new util.BitSet(mask.size)
    tmp.or(mask)
    new BitMaskSet(tmp)
  }
}

class BitMaskSetIterator(mask: util.BitSet) extends SetIterator {
  private[this] var pos = mask.nextSetBit(0)

  def hasNext: Boolean = (pos >= 0)

  def next(): Int = {
    if (pos < 0) LazySet.END else {
      val tmp = pos
      pos = mask.nextSetBit(pos + 1)
      tmp
    }
  }

  def skipTo(k: Int): Boolean = {
    val found = (k >= 0 && k < mask.size) && mask.get(k)
    pos = if (k + 1 < mask.size) mask.nextSetBit(k + 1) else -1
    found
  }

  def maxSize: Int = mask.size
}

class SortedArraySet(buf: Array[Int], length: Int) extends AbstractLazySet {
  ///require(length > 0, "length of array must be >= 0")

  def contains(k: Int): Boolean = {
    util.Arrays.binarySearch(buf, 0, length, k) >= 0
  }

  def iterator: SetIterator = {
    new ArraySetIterator(buf, length)
  }

  lazy val depth: Int = 0

  def maxSize: Int = length

  def maxValue: Int = if (length > 0) buf(length - 1) else LazySet.END
}

class ArraySetIterator(buf: Array[Int], length: Int) extends SetIterator {
  private[this] val end = length - 1
  private[this] var pos = -1

  def hasNext: Boolean = (pos < end)

  def next(): Int = {
    pos += 1
    if (pos < length) buf(pos) else LazySet.END
  }

  @scala.annotation.tailrec
  private def binarySearchTo(k: Int, s: Int, e: Int): Boolean = {
    val m = (e + s) / 2
    val cmp = buf(m) - k
    if (cmp == 0 || s >= e) {
      pos = if (cmp <= 0) m else m - 1
      cmp == 0
    } else if (cmp > 0) {
      binarySearchTo(k, s, m - 1)
    } else {
      binarySearchTo(k, m + 1, e)
    }
  }

  private def scanTo(k: Int): Boolean = {
    pos += 1
    while (pos < length && buf(pos) < k) {
      pos += 1
    }
    val matches = (pos < length && buf(pos) == k)
    if (!matches) pos -= 1
    matches
  }

  def skipTo(k: Int): Boolean = {
    val maxDist = buf.length - pos
    if (maxDist < 15 || buf(pos + 10) > k) {
      scanTo(k)
    } else {
      binarySearchTo(k, pos + 1, buf.length - 1)
    }
  }

  def maxSize: Int = buf.length
}

class OffsetSet(s: LazySet, offset: Int) extends AbstractLazySet {
  def contains(k: Int): Boolean = {
    k > offset && s.contains(k)
  }

  def iterator: SetIterator = {
    new OffsetSetIterator(s.iterator, offset)
  }

  def depth: Int = s.depth + 1

  def maxSize: Int = s.maxSize

  def maxValue: Int = s.maxValue
}

class OffsetSetIterator(s: SetIterator, offset: Int) extends SetIterator {
  s.skipTo(offset)

  def hasNext: Boolean = s.hasNext

  def next(): Int = s.next()

  def skipTo(k: Int): Boolean = s.skipTo(k)

  def maxSize: Int = s.maxSize
}

class DiffSet(s1: LazySet, s2: LazySet)
    extends AbstractLazySet {

  def contains(k: Int): Boolean = {
    s1.contains(k) && !s2.contains(k)
  }

  def iterator: SetIterator = {
    new DiffSetIterator(s1.iterator, s2.iterator)
  }

  lazy val depth: Int = {
    val d1 = s1.depth
    val d2 = s2.depth
    val max = if (d1 <= d2) d2 else d1
    max + 1
  }

  def maxSize: Int = s1.maxSize

  def maxValue: Int = math.max(s1.maxValue, s2.maxValue)
}

class DiffSetIterator(s1: SetIterator, s2: SetIterator) extends SetIterator {
  private[this] var current = nextValue()

  @scala.annotation.tailrec
  private def nextValue(): Int = {
    if (s1.hasNext) {
      if (s2.hasNext) {
        val v = s1.next()
        if (s2.skipTo(v)) nextValue() else v
      } else {
        s1.next()
      }
    } else {
      LazySet.END
    }
  }

  def hasNext: Boolean = (current != LazySet.END)

  def next(): Int = {
    val prev = current
    current = nextValue()
    prev
  }

  def skipTo(k: Int): Boolean = {
    while (current < k) {
      current = nextValue()
    }
    val matches = (current == k)
    if (matches) current = nextValue()
    matches
  }

  def maxSize: Int = s1.maxSize
}

class UnionSet(s1: LazySet, s2: LazySet)
    extends AbstractLazySet {

  def contains(k: Int): Boolean = {
    s1.contains(k) || s2.contains(k)
  }

  def iterator: SetIterator = {
    new UnionSetIterator(s1.iterator, s2.iterator)
  }

  lazy val depth: Int = {
    val d1 = s1.depth
    val d2 = s2.depth
    val max = if (d1 <= d2) d2 else d1
    max + 1
  }

  def maxSize: Int = {
    s1.maxSize + s2.maxSize
  }

  def maxValue: Int = math.max(s1.maxValue, s2.maxValue)
}

object UnionSetIterator {
  private final val BOTH = 0
  private final val ONE = 2
  private final val TWO = 1
}

class UnionSetIterator(s1: SetIterator, s2: SetIterator)
    extends SetIterator {

  import com.netflix.atlas.core.index.UnionSetIterator._

  private[this] var mode = BOTH
  private[this] var current = -1

  def hasNext: Boolean = {
    s1.hasNext || s2.hasNext || (mode != BOTH && current != LazySet.END)
  }

  def next(): Int = {
    (mode: @scala.annotation.switch) match {
      case BOTH => pickNext(s1.next(), s2.next())
      case ONE  => pickNext(s1.next(), current)
      case TWO  => pickNext(current, s2.next())
      case _    => LazySet.END
    }
  }

  @inline private final def pickNext(c1: Int, c2: Int): Int = {
    /*if (c1 == c2) {
      mode = BOTH
      c1
    } else if (c1 < c2) {
      current = c2
      mode = ONE
      c1
    } else {
      current = c1
      mode = TWO
      c2
    }*/
    val diff = c1 - c2
    val mask = (diff >> 31)
    val abs = (diff + mask) ^ mask
    mode = (diff >>> 31) + ((Integer.MAX_VALUE + abs) >>> 31)
    val tmp = diff & mask
    current = c1 - tmp // max
    c2 + tmp // min
  }

  def skipTo(k: Int): Boolean = {
    val m1 = s1.skipTo(k)
    val m2 = s2.skipTo(k)
    mode = BOTH
    m1 || m2
  }

  def maxSize: Int = s1.maxSize + s2.maxSize
}

class IntersectSet(s1: LazySet, s2: LazySet) extends AbstractLazySet {

  def contains(k: Int): Boolean = {
    s1.contains(k) && s2.contains(k)
  }

  def iterator: SetIterator = {
    if (s1.maxSize < s2.maxSize)
      new IntersectSetIterator(s1.iterator, s2.iterator)
    else
      new IntersectSetIterator(s2.iterator, s1.iterator)
  }

  lazy val depth: Int = {
    val d1 = s1.depth
    val d2 = s2.depth
    val max = if (d1 <= d2) d2 else d1
    max + 1
  }

  def maxSize: Int = {
    val sz1 = s1.maxSize
    val sz2 = s2.maxSize
    if (sz1 <= sz2) sz1 else sz2
  }

  def maxValue: Int = math.max(s1.maxValue, s2.maxValue)
}

class IntersectSetIterator(s1: SetIterator, s2: SetIterator) extends SetIterator {
  private[this] var current = nextValue()

  @scala.annotation.tailrec
  private def nextValue(): Int = {
    if (s1.hasNext && s2.hasNext) {
      val v = s1.next()
      if (s2.skipTo(v)) v else nextValue()
    } else {
      LazySet.END
    }
  }

  def hasNext: Boolean = (current != LazySet.END)

  def next(): Int = {
    val prev = current
    current = nextValue()
    prev
  }

  def skipTo(k: Int): Boolean = {
    while (current < k) {
      current = nextValue()
    }
    val matches = (current == k)
    if (matches) current = nextValue()
    matches
  }

  def maxSize: Int = {
    val sz1 = s1.maxSize
    val sz2 = s2.maxSize
    if (sz1 <= sz2) sz1 else sz2
  }
}
