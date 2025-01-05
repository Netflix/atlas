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
package com.netflix.atlas.core.util

import java.util.Comparator
import java.util.PriorityQueue

/**
  * Fixed size buffer that can be used for computing the top-K items.
  *
  * @param maxSize
  *     Maximum size of the buffer.
  * @param comparator
  *     Comparator used for checking the relative priority of entries.
  */
class BoundedPriorityBuffer[T <: AnyRef](maxSize: Int, comparator: Comparator[T]) {

  require(maxSize > 0, "maxSize must be > 0")

  private val queue = new PriorityQueue[T](comparator.reversed())

  /**
    * Add a value into the buffer if there is space or it has a higher priority than the
    * lowest priority item currently in the buffer. If it has the same priority as the lowest
    * priority item, then the previous value will be retained and the new value will be
    * rejected.
    *
    * @param value
    *     Value to attempt to add into the buffer.
    * @return
    *     The return value is either: a) a value that was ejected because of the new addition,
    *     b) the value that was passed in if it wasn't high enough priority, or c) `null` if
    *     the max size has not yet been reached.
    */
  def add(value: T): T = {
    if (queue.size == maxSize) {
      // Buffer is full, check if the new value is higher priority than the lowest priority
      // item in the heap
      val lowestPriorityItem = queue.peek()
      if (comparator.compare(value, lowestPriorityItem) < 0) {
        queue.poll()
        queue.offer(value)
        lowestPriorityItem
      } else {
        value
      }
    } else {
      queue.offer(value)
      null.asInstanceOf[T]
    }
  }

  /** Number of items in the buffer. */
  def size: Int = {
    queue.size
  }

  /** Invoke the function `f` for all items in the buffer. */
  def foreach(f: T => Unit): Unit = {
    queue.forEach(v => f(v))
  }

  /** Return a list containing all of the items in the buffer. */
  def toList: List[T] = {
    val builder = List.newBuilder[T]
    val it = queue.iterator()
    while (it.hasNext) {
      builder += it.next()
    }
    builder.result()
  }

  /** Drain elements to a list that is ordered by priority. */
  def drainToOrderedList(): List[T] = {
    val builder = List.newBuilder[T]
    while (!queue.isEmpty) {
      builder += queue.poll()
    }
    builder.result()
  }
}
