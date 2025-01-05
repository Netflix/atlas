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

/** Helper functions for tags. */
object Tag {

  def apply(key: String, value: String): Tag = {
    Tag(key, value, -1)
  }
}

/**
  * Represents a key/value pair and it's associated count. The count is the number of items that
  * are marked with the tag.
  *
  * @param key    key for the tag
  * @param value  value associated with the key
  * @param count  number of items with this tag or -1 if unknown
  */
case class Tag(key: String, value: String, count: Int) extends Comparable[Tag] {

  def <(t: Tag): Boolean = compareTo(t) < 0

  def >(t: Tag): Boolean = compareTo(t) > 0

  /** Tags are ordered by key, value, and then count. */
  def compareTo(t: Tag): Int = {
    val k = key.compareTo(t.key)
    if (k != 0) k
    else {
      val v = value.compareTo(t.value)
      if (v != 0) v else count - t.count
    }
  }
}
