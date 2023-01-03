/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.webapi

import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.model.ItemId

trait PublishConsumer {

  def consume(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit
}

object PublishConsumer {

  /**
    * Returns a new consumer that will accumulate the values into a list of data points.
    */
  def datapointList: ListPublishConsumer = {
    new ListPublishConsumer
  }

  class ListPublishConsumer extends PublishConsumer {

    private val builder = List.newBuilder[DatapointTuple]

    def consume(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit = {
      builder += DatapointTuple(id, tags, timestamp, value)
    }

    def toList: List[DatapointTuple] = {
      builder.result()
    }
  }
}
