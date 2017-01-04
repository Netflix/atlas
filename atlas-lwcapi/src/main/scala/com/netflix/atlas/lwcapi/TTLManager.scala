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
package com.netflix.atlas.lwcapi

import scala.collection.mutable
import scala.annotation.tailrec

//
// We store timestamps as negative values, since our priority queue returns the largest
// value, and thus will return the ones we need to check (the smaller negative values)
//
class TTLManager[K](implicit val ord: Ordering[K]) {
  private val prio = mutable.PriorityQueue[(Long, K)]()
  private val touched = mutable.Map[K, Long]()

  def lastTouched(key: K): Long = {
    touched.getOrElse(key, 0)
  }

  def touch(key: K, now: Long): Unit = {
    val old = touched.put(key, now)
    if (old.isEmpty)
      prio += (-now -> key)
  }

  def remove(key: K): Unit = {
    touched.remove(key)
  }

  @tailrec final def needsTouch(since: Long): Option[K] = {
    val next = prio.headOption
    if (next.isEmpty) None else {
      val key = next.get._2
      val item = lastTouched(key)
      if (item == 0) {
        // we don't care about this one, drop it
        prio.dequeue
        needsTouch(since)
      } else if (-item != next.get._1) {
        // requeue with the latest timestamp
        prio.dequeue
        prio += (-item -> key)
        needsTouch(since)
      } else if (item > since) None else {
        prio.dequeue
        touched.remove(key)
        Some(key)
      }
    }
  }
}
