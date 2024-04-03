/*
 * Copyright 2014-2024 Netflix, Inc.
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

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.json.JsonSupport

/**
  * Raw event data to pass through to the consumer.
  *
  * @param id
  *     Identifies the expression that resulted in this datapoint being generated.
  * @param payload
  *     Raw event payload.
  */
case class LwcEvent(id: String, payload: JsonNode) extends JsonSupport {

  require(id != null, "id cannot be null")
  val `type`: String = "event"
}
