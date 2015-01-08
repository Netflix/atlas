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
package com.netflix.atlas.core.db

import com.netflix.atlas.core.model.LazyTaggedItem
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.model.TimeSeries

case class BlockStoreItem(tags: Map[String, String], blocks: BlockStore) extends LazyTaggedItem {

  def label: String = TimeSeries.toLabel(tags)

  override def isExpired: Boolean = !blocks.hasData
}

object BlockStoreItem {

  import com.netflix.atlas.core.util.InternMap

  private val metricInterner = InternMap.concurrent[BlockStoreItem](1000000)

  def create(tags: Map[String, String], blocks: BlockStore): BlockStoreItem = {
    val m = new BlockStoreItem(tags, blocks)
    metricInterner.intern(m)
  }

  def retain(keep: Long => Boolean) {
    TaggedItem.retain(keep)
    metricInterner.retain(keep)
  }
}
