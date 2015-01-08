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

import java.math.BigInteger
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.spectator.api.Spectator

import scala.reflect.ClassTag


object BatchUpdateTagIndex {
  def newLazyIndex[T <: TaggedItem: ClassTag]: BatchUpdateTagIndex[T] = {
    new BatchUpdateTagIndex[T](items => new CachingTagIndex(new LazyTagIndex(items)))
  }
}

/**
 * Mutable tag index that batches updates and atomically swaps in a new index in the background at
 * a configured interval.
 *
 * @param newIndex  function to create a new index from the set of items
 */
class BatchUpdateTagIndex[T <: TaggedItem: ClassTag](newIndex: Array[T] => TagIndex[T])
    extends MutableTagIndex[T] {

  private def registry = Spectator.registry()

  // Current index used for all query operations
  private val currentIndex = new AtomicReference[TagIndex[T]](newIndex(Array.empty[T]))

  // Last time the index was rebuilt
  private val lastRebuildTime = new AtomicLong(0L)

  // Updates that have not yet been added to the current index
  private val pendingUpdates =
    registry.collectionSize("atlas.index.pendingUpdates", new LinkedBlockingQueue[T])

  private val rebuildTimer = registry.longTaskTimer("atlas.index.rebuildTime")

  /** Returns the timestamp for when the current index was built. */
  def buildTime: Long = lastRebuildTime.get

  /** Returns the number of pending updates. */
  def numPending: Int = pendingUpdates.size

  /**
   * Rebuild the index to include all of the pending updates that have accumulated. Any expired
   * items will also be removed.
   */
  def rebuildIndex() {
    val timerId = rebuildTimer.start()
    try {
      import scala.collection.JavaConversions._

      // Drain the update queue and create map of items for deduping, we put new items in the
      // map first so that an older item, if present, will be preferred
      val size = pendingUpdates.size
      val updates = new java.util.ArrayList[T](size)
      pendingUpdates.drainTo(updates, size)
      val items = new java.util.HashMap[BigInteger, T]
      updates.foreach { i => items.put(i.id, i) }

      // Get set of all items in the current index that are not expired
      val matches = currentIndex.get.findItems(TagQuery(None)).filter(!_.isExpired)
      matches.foreach { i => items.put(i.id, i) }

      // Create array of items and build the index
      rebuildIndex(items.values.toList)
    } finally {
      rebuildTimer.stop(timerId)
    }
  }

  def rebuildIndex(items: List[T]) {
    currentIndex.set(newIndex(items.toArray))
    lastRebuildTime.set(System.currentTimeMillis)
  }

  def update(item: T) {
    pendingUpdates.add(item)
  }

  def update(items: List[T]) {
    items.foreach(i => pendingUpdates.add(i))
  }

  def findTags(query: TagQuery): List[Tag] = currentIndex.get.findTags(query)

  def findKeys(query: TagQuery): List[TagKey] = currentIndex.get.findKeys(query)

  def findValues(query: TagQuery): List[String] = currentIndex.get.findValues(query)

  def findItems(query: TagQuery): List[T] = currentIndex.get.findItems(query)

  def size: Int = currentIndex.get.size
}

