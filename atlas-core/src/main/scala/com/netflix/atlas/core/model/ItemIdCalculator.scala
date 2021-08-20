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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.SmallHashMap

import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets

/**
  * Helper to compute an ItemId from a tag map. This class will reuse buffers and is not
  * thread-safe.
  */
class ItemIdCalculator {

  import ItemIdCalculator._

  private val md = Hash.get("SHA1")
  private val enc = StandardCharsets.UTF_8.newEncoder
  private var cbuf = CharBuffer.allocate(maxLength)
  private var buf = ByteBuffer.allocate(maxLength * 2)
  private var pairs = new Array[String](maxTags)

  private def writePair(k: String, v: String): Unit = {
    // The additional 2 are for separator characters that are used
    val spaceNeeded = k.length + v.length + 2
    if (cbuf.remaining() < spaceNeeded) {
      cbuf.flip()
      val tmp = CharBuffer.allocate(2 * (cbuf.capacity() + spaceNeeded))
      tmp.put(cbuf)
      cbuf = tmp
    }
    cbuf.put(k)
    cbuf.put('=')
    cbuf.put(v)
  }

  private def toSortedArray(tags: Map[String, String]): Array[String] = {
    val length = tags.size * 2
    if (length > pairs.length) {
      pairs = new Array[String](length)
    }

    tags match {
      case ts: SmallHashMap[String, String] =>
        var pos = 0
        val iter = ts.entriesIterator
        while (iter.hasNext) {
          pairs(pos) = iter.key
          pairs(pos + 1) = iter.value
          iter.nextEntry()
          pos += 2
        }
      case _ =>
        var pos = 0
        tags.foreachEntry { (k, v) =>
          pairs(pos) = k
          pairs(pos + 1) = v
          pos += 2
        }
    }

    insertionSort(pairs, length)
    pairs
  }

  /**
    * Compute an identifier for a set of tags. The id is a sha1 hash of a normalized string
    * representation. Identical tags will always get the same id.
    */
  def compute(tags: Map[String, String]): ItemId = {

    if (tags.isEmpty) emptyId
    else {
      md.reset()
      enc.reset()
      cbuf.clear()
      buf.clear()

      val length = tags.size * 2
      val pairs = toSortedArray(tags)

      writePair(pairs(0), pairs(1))
      var pos = 2
      while (pos < length) {
        cbuf.put(',')
        writePair(pairs(pos), pairs(pos + 1))
        pos += 2
      }

      cbuf.flip()
      if (buf.capacity() < 2 * cbuf.capacity()) {
        buf = ByteBuffer.allocate(2 * cbuf.capacity())
      }
      enc.encode(cbuf, buf, true)
      buf.flip()
      md.update(buf)
      ItemId(md.digest)
    }
  }
}

object ItemIdCalculator {

  type Pair = (String, String)

  private val emptyId = ItemId(Hash.sha1bytes(""))

  // Large enough for most key/value pairs
  private val maxLength = 2048

  // Large enough for typical tag maps
  private val maxTags = 128

  // Thread local to allow reuse of calculators
  private val calculators = ThreadLocal.withInitial[ItemIdCalculator] { () =>
    new ItemIdCalculator
  }

  /**
    * Compute an identifier for a set of tags. The id is a sha1 hash of a normalized string
    * representation. Identical tags will always get the same id.
    */
  def compute(tags: Map[String, String]): ItemId = {
    calculators.get().compute(tags)
  }

  /**
    * Sort a string array that consists of tag key/value pairs by key. The array will be
    * sorted in-place. Tag lists are supposed to be fairly small, typically less than 20
    * tags. With the small size a simple insertion sort works well.
    */
  private def insertionSort(ts: Array[String], length: Int): Unit = {
    if (length == 4) {
      // Two key/value pairs, swap if needed
      if (ts(0).compareTo(ts(2)) > 0) {
        // Swap key
        var tmp = ts(0)
        ts(0) = ts(2)
        ts(2) = tmp
        // Swap value
        tmp = ts(1)
        ts(1) = ts(3)
        ts(3) = tmp
      }
    } else if (length > 4) {
      // One entry is already sorted. Two entries handled above, for larger arrays
      // use insertion sort.
      var i = 2
      while (i < length) {
        val k = ts(i)
        val v = ts(i + 1)
        var j = i - 2

        while (j >= 0 && ts(j).compareTo(k) > 0) {
          ts(j + 2) = ts(j)
          ts(j + 3) = ts(j + 1)
          j -= 2
        }
        ts(j + 2) = k
        ts(j + 3) = v

        i += 2
      }
    }
  }
}
