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
package com.netflix.atlas.eval.model

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport

/**
  * Datapoint read in from the LWC service.
  *
  * @param timestamp
  *     Timestamp for the value. It should already be normalized to the step interval
  *     for the data stream.
  * @param id
  *     Identifies the expression that resulted in this datapoint being generated. See
  *     [[AggrDatapoint]] for more information.
  * @param tags
  *     Tags associated with the datapoint.
  * @param value
  *     Value for the datapoint.
  * @param samples
  *     Optional set of event samples associated with the message. Typically used when
  *     mapping events into a count with a few sample messages.
  */
case class LwcDatapoint(
  timestamp: Long,
  id: String,
  tags: Map[String, String],
  value: Double,
  samples: List[List[Any]] = Nil
) extends JsonSupport {

  val `type`: String = "datapoint"

  override def hasCustomEncoding: Boolean = true

  override def encode(gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeStringField("type", `type`)
    gen.writeNumberField("timestamp", timestamp)
    gen.writeStringField("id", id)
    gen.writeObjectFieldStart("tags")
    tags.foreachEntry(gen.writeStringField)
    gen.writeEndObject()
    gen.writeNumberField("value", value)
    if (samples.nonEmpty) {
      gen.writeFieldName("samples")
      Json.encode(gen, samples)
    }
    gen.writeEndObject()
  }
}
