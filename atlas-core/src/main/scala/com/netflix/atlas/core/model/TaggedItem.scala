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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.InternMap
import com.netflix.atlas.core.util.Interner
import com.netflix.atlas.core.util.SortedTagMap

/**
  * Helper functions for manipulating tagged items.
  */
object TaggedItem {

  type Pair = (String, String)

  private val initCapacity = 1000000
  private val idInterner = InternMap.concurrent[ItemId](initCapacity)
  private val tagsInterner = InternMap.concurrent[Map[String, String]](initCapacity)

  /**
    * Compute an identifier for a set of tags. The id is a sha1 hash of a normalized string
    * representation. Identical tags will always get the same id.
    */
  def computeId(tags: Map[String, String]): ItemId = {
    ItemIdCalculator.compute(tags)
  }

  /**
    * Compute the id and return an interned copy of the value. This function should be used if
    * keeping metric data in memory for a long time to avoid redundant big integer objects hanging
    * around.
    */
  def createId(tags: Map[String, String]): ItemId = {
    val id = computeId(tags)
    idInterner.intern(id)
  }

  def internId(id: ItemId): ItemId = {
    idInterner.intern(id)
  }

  def internTags(tags: Map[String, String]): Map[String, String] = {
    val strInterner = Interner.forStrings
    val iter = tags.iterator.map { t =>
      strInterner.intern(t._1) -> strInterner.intern(t._2)
    }
    val smallMap = SortedTagMap(iter)
    tagsInterner.intern(smallMap)
  }

  def internTagsShallow(tags: Map[String, String]): Map[String, String] = {
    tagsInterner.intern(tags)
  }

  def retain(keep: Long => Boolean): Unit = {
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
  def id: ItemId

  /** Standard string representation of the id. */
  def idString: String = id.toString

  /** The tags associated with this item. */
  def tags: Map[String, String]

  /** Returns true if the item is expired and no data is available. */
  def isExpired: Boolean = false

  /**
    * Code that just needs to iterate over all tags should use this method. Allows for
    * implementations to optimize how the tag data is stored and traversed.
    */
  def foreach(f: (String, String) => Unit): Unit = {
    tags.foreachEntry(f)
  }
}

trait LazyTaggedItem extends TaggedItem {
  lazy val id: ItemId = TaggedItem.computeId(tags)
}

case class BasicTaggedItem(tags: Map[String, String]) extends LazyTaggedItem
