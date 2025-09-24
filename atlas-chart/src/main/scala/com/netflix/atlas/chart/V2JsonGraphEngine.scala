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
package com.netflix.atlas.chart

import java.io.OutputStream

import com.netflix.atlas.chart.model.GraphDef

/**
  * Experimental variant of json encoding. The format is not yet final and may change. Goals:
  *
  * - Include all metadata so the output can be used to precisely recreate the image. This is
  *   useful for debugging and allows dynamic rendering to be more precise.
  *
  * - Must be parseable with a standard json parser. The default json format encodes NaN and
  *   Infinity without quotes so it cannot be used without parser extensions. The std.json format
  *   fixes that, but in all new formats they should be standard for the json encoding.
  *
  * - Allow the data to be incrementally returned. With many of the existing formats the entire
  *   payload must be ready before we can output data. As data volumes get bigger it is
  *   necessary to be able to emit data as it becomes available.
  *
  * - Allow for incremental evaluation of the data over time. The constant metadata for the
  *   chart should be written out first. Data can arrive over time. For example, in a streaming
  *   execution we might get data for all lines for a given minute, then the next minute, and
  *   so on.
  *
  * - Consistent where possible to the SSE payloads from the fetch API.
  */
class V2JsonGraphEngine extends GraphEngine {

  override def name: String = "v2.json"

  override def contentType: String = "application/json"

  override def write(config: GraphDef, output: OutputStream): Unit = {
    JsonCodec.encode(output, config)
  }
}
