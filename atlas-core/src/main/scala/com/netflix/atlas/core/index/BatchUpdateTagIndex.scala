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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.LongTaskTimer
import com.netflix.spectator.api.patterns.PolledMeter

import java.util.Comparator
import scala.reflect.ClassTag

object BatchUpdateTagIndex {

  def newRoaringIndex[T <: TaggedItem: ClassTag](registry: Registry): BatchUpdateTagIndex[T] = {
    val stats = new IndexStats(registry)
    new BatchUpdateTagIndex[T](registry, items => new RoaringTagIndex(items, stats))
  }
}

/**
  * Mutable tag index that batches updates and atomically swaps in a new index in the background at
  * a configured interval.
  *
  * @param registry
  *     Spectator registry to use for reporting metrics.
  * @param newIndex
  *     Function to create a new index from the set of items.
  */
class BatchUpdateTagIndex[T <: TaggedItem: ClassTag](
  registry: Registry,
  newIndex: Array[T] => TagIndex[T]
) extends MutableTagIndex[T] {

  // Current index used for all query operations
  private val currentIndex = new AtomicReference[TagIndex[T]](newIndex(Array.empty[T]))

  // Last time the index was rebuilt
  private val lastRebuildTime = new AtomicLong(0L)

  // Updates that have not yet been added to the current index
  private val pendingUpdates = PolledMeter
    .using(registry)
    .withName("atlas.index.pendingUpdates")
    .monitorSize(new LinkedBlockingQueue[T])

  private val rebuildTimer =
    LongTaskTimer.get(registry, registry.createId("atlas.index.rebuildTime"))

  /** Returns the timestamp for when the current index was built. */
  def buildTime: Long = lastRebuildTime.get

  /** Returns the number of pending updates. */
  def numPending: Int = pendingUpdates.size

  /**
    * Rebuild the index to include all of the pending updates that have accumulated. Any expired
    * items or items that do not match the filter will also be removed. Filtered out ids will be
    * returned.
    */
  def rebuildIndex(filter: Query = Query.True): List[T] = {
    val timerId = rebuildTimer.start()
    try {
      // Drain the update queue and create map of items for deduping, we put new items in the
      // map first so that an older item, if present, will be preferred
      val size = pendingUpdates.size
      val updates = new java.util.ArrayList[T](size)
      pendingUpdates.drainTo(updates, size)
      val items = new java.util.HashMap[ItemId, T](size)
      updates.forEach { i =>
        if (filter.matches(i.tags))
          items.put(i.id, i)
      }

      // Get set of all items in the current index that are not expired and match the filter
      val currentItems = currentIndex.get
        .findItems(TagQuery(None))
        .filter(i => !i.isExpired)

      // Remove items that are already indexed from the set of new items
      currentItems.foreach { i =>
        items.remove(i.id)
      }

      // Find set of items matching and not matching the filter
      val (matches, nonMatches) = currentItems.partition(item => filter.matches(item.tags))

      // Merge previous with new updates. Previous matches are coming from index and
      // will already be sorted by the id.
      val a1 = matches.toArray
      val a2 = items.values.toArray(new Array[T](0))
      java.util.Arrays.sort(a2, RoaringTagIndex.IdComparator)
      val dst = new Array[T](a1.length + a2.length)
      ArrayHelper.merge[T](
        RoaringTagIndex.IdComparator.asInstanceOf[Comparator[T]],
        (a, _) => a,
        a1,
        a1.length,
        a2,
        a2.length,
        dst
      )

      // Create array of items and build the index
      rebuildIndex(dst)

      // Return set of items that have been filtered out
      nonMatches
    } finally {
      rebuildTimer.stop(timerId)
    }
  }

  def rebuildIndex(items: Array[T]): Unit = {
    currentIndex.set(newIndex(items))
    lastRebuildTime.set(registry.clock().wallTime())
  }

  def rebuildIndex(items: List[T]): Unit = {
    rebuildIndex(items.toArray)
  }

  def update(item: T): Unit = {
    pendingUpdates.add(item)
  }

  def update(items: List[T]): Unit = {
    items.foreach(i => pendingUpdates.add(i))
  }

  def findTags(query: TagQuery): List[Tag] = currentIndex.get.findTags(query)

  def findKeys(query: TagQuery): List[String] = currentIndex.get.findKeys(query)

  def findValues(query: TagQuery): List[String] = currentIndex.get.findValues(query)

  def findItems(query: TagQuery): List[T] = currentIndex.get.findItems(query)

  override def iterator: Iterator[T] = currentIndex.get.iterator

  def size: Int = currentIndex.get.size
}
