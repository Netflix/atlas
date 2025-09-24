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

import com.fasterxml.jackson.core.JsonFactory
import com.netflix.atlas.chart.model.GraphDef

object GraphEngine {
  val jsonFactory = new JsonFactory
}

trait GraphEngine {

  def name: String

  def contentType: String

  def write(config: GraphDef, output: OutputStream): Unit
}
