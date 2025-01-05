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
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport

/**
  * Message type use for events to forward to a consumer.
  */
case class EventMessage(payload: JsonNode) extends JsonSupport {

  override def encode(gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeStringField("type", "event")
    gen.writeFieldName("payload")
    Json.encode(gen, payload)
    gen.writeEndObject()
  }
}
