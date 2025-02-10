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
package com.netflix.atlas.lwc.events

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.json.Json

/**
  * Event that represents a time series data point generated from the events.
  *
  * @param id
  *     Id for the associated subscription.
  * @param tags
  *     Tags for the data point.
  * @param timestamp
  *     Timestamp for the data point.
  * @param value
  *     Value for the data point.
  * @param samples
  *     Optional set of event samples associated with the message.
  */
case class DatapointEvent(
  id: String,
  tags: Map[String, String],
  timestamp: Long,
  override val value: Double,
  samples: List[List[Any]] = Nil
) extends LwcEvent {

  override def rawEvent: Any = this

  override def extractValue(key: String): Any = {
    tags.getOrElse(key, null)
  }

  override def encode(gen: JsonGenerator): Unit = {
    Json.encode(gen, this)
  }

  override def encodeAsRow(columns: List[String], gen: JsonGenerator): Unit = {
    gen.writeStartArray()
    encodeColumns(columns, gen)
    gen.writeEndArray()
  }

  @scala.annotation.tailrec
  private def encodeColumns(columns: List[String], gen: JsonGenerator): Unit = {
    if (columns.nonEmpty) {
      Json.encode(gen, extractValueSafe(columns.head))
      encodeColumns(columns.tail, gen)
    }
  }
}
