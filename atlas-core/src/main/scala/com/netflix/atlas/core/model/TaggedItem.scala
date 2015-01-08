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
package com.netflix.atlas.core.model

import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.Charset
import java.nio.charset.CharsetEncoder
import java.security.MessageDigest
import java.util

import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.InternMap
import com.netflix.atlas.core.util.Interner
import com.netflix.atlas.core.util.SmallHashMap


/**
 * Helper functions for manipulating tagged items.
 */
object TaggedItem {

  import java.util.Comparator

  type Pair = (String, String)

  private val emptyId = Hash.sha1("")

  private val initCapacity = 1000000
  private val idInterner = InternMap.concurrent[BigInteger](initCapacity)
  private val tagsInterner = InternMap.concurrent[Map[String, String]](initCapacity)

  private val keyComparator = new Comparator[Pair] {
    def compare(t1: Pair, t2: Pair): Int = {
      t1._1.compareTo(t2._1)
    }
  }

  private def writePair(p: Pair, buf: ByteBuffer, enc: CharsetEncoder, md: MessageDigest) {
    enc.encode(CharBuffer.wrap(p._1), buf, true)
    buf.flip()
    md.update(buf)
    buf.clear()

    md.update('='.asInstanceOf[Byte])

    enc.encode(CharBuffer.wrap(p._2), buf, true)
    buf.flip()
    md.update(buf)
    buf.clear()
  }

  /**
   * Compute an identifier for a set of tags. The id is a sha1 hash of a normalized string
   * representation. Identical tags will always get the same id.
   */
  def computeId(tags: Map[String, String]): BigInteger = {
    if (tags.isEmpty) emptyId else {

      val pairs = new Array[Pair](tags.size)
      val it = tags.iterator
      var pos = 0
      var maxLength = 0
      while (it.hasNext) {
        val t = it.next()
        pairs(pos) = t
        pos += 1
        maxLength = if (t._1.length > maxLength) t._1.length else maxLength
        maxLength = if (t._2.length > maxLength) t._2.length else maxLength
      }

      util.Arrays.sort(pairs, keyComparator)

      val md = Hash.get("SHA1")
      val enc = Charset.forName("UTF-8").newEncoder
      val buf = ByteBuffer.allocate(maxLength * 2)

      writePair(pairs(0), buf, enc, md)
      pos = 1
      while (pos < pairs.length) {
        md.update(','.asInstanceOf[Byte])
        writePair(pairs(pos), buf, enc, md)
        pos += 1
      }
      new BigInteger(1, md.digest)
    }
  }

  /**
   * Compute the id and return an interned copy of the value. This function should be used if
   * keeping metric data in memory for a long time to avoid redundant big integer objects hanging
   * around.
   */
  def createId(tags: Map[String, String]): BigInteger = {
    val id = computeId(tags)
    idInterner.intern(id)
  }

  def internId(id: BigInteger): BigInteger = {
    idInterner.intern(id)
  }

  def internTags(tags: Map[String, String]): Map[String, String] = {
    val strInterner = Interner.forStrings
    val iter = tags.iterator.map { t =>
      strInterner.intern(t._1) -> strInterner.intern(t._2)
    }
    val smallMap = SmallHashMap(tags.size, iter)
    tagsInterner.intern(smallMap)
  }

  private[model] def internTagsShallow(tags: Map[String, String]): Map[String, String] = {
    tagsInterner.intern(tags)
  }

  def retain(keep: Long => Boolean) {
    idInterner.retain(keep)
    tagsInterner.retain(keep)
  }

  /**
   * Compute the new tags for the aggregate buffer. The tags are the intersection of tag values.
   */
  def aggrTags(t1: Map[String, String], t2: Map[String, String]): Map[String, String] = {
    t1.toSet.intersect(t2.toSet).toMap
  }
}

/**
 * Represents an item that can be searched for using a set of tags.
 */
trait TaggedItem {
  /** Unique id based on the tags. */
  def id: BigInteger

  /** Standard string representation of the id. */
  def idString: String = "%040x".format(id)

  /** The tags associated with this item. */
  def tags: Map[String, String]

  /** Returns true if the item is expired and no data is available. */
  def isExpired: Boolean = false

  /**
   * Code that just needs to iterate over all tags should use this method. Allows for
   * implementations to optimize how the tag data is stored and traversed.
   */
  def foreach(f: (String, String) => Unit) {
    tags match {
      case m: SmallHashMap[String, String] => m.foreachItem(f)
      case m: Map[String, String]          => m.foreach { t => f(t._1, t._2) }
    }
  }
}

trait LazyTaggedItem extends TaggedItem {
  lazy val id: BigInteger = TaggedItem.computeId(tags)
}

case class BasicTaggedItem(tags: Map[String, String]) extends LazyTaggedItem

