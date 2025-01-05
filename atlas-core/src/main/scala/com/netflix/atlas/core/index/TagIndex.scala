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

import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TaggedItem

trait TagIndex[T <: TaggedItem] {

  def findTags(query: TagQuery): List[Tag]

  def findKeys(query: TagQuery): List[String]

  def findValues(query: TagQuery): List[String]

  def findItems(query: TagQuery): List[T]

  def iterator: Iterator[T]

  def size: Int
}

trait MutableTagIndex[T <: TaggedItem] extends TagIndex[T] {

  /** Update the index with the given items. */
  def update(items: List[T]): Unit
}
