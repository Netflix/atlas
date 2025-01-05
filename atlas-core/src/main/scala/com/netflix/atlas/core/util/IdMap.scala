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

import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Utils

/**
  * Wraps a Spectator Id so it can be used as Scala Map. Modifications will result
  * in a different map type being returned.
  */
final case class IdMap(id: Id) extends scala.collection.immutable.Map[String, String] {

  override def removed(key: String): Map[String, String] = {
    SortedTagMap(this).removed(key)
  }

  override def updated[V1 >: String](key: String, value: V1): Map[String, V1] = {
    SortedTagMap(this).updated(key, value)
  }

  override def get(key: String): Option[String] = {
    if (key == "name")
      Some(id.name())
    else
      Option(Utils.getTagValue(id, key))
  }

  override def iterator: Iterator[(String, String)] = {
    new Iterator[(String, String)] {
      private var i = 0

      override def hasNext: Boolean = i < id.size()

      override def next(): (String, String) = {
        val tuple = id.getKey(i) -> id.getValue(i)
        i += 1
        tuple
      }
    }
  }

  override def foreachEntry[U](f: (String, String) => U): Unit = {
    val size = id.size()
    var i = 0
    while (i < size) {
      f(id.getKey(i), id.getValue(i))
      i += 1
    }
  }
}
