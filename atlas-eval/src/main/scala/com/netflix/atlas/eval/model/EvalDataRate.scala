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

import com.netflix.atlas.json.JsonSupport

case class EvalDataRate(
  timestamp: Long,
  step: Long,
  inputSize: EvalDataSize,
  intermediateSize: EvalDataSize,
  outputSize: EvalDataSize
) extends JsonSupport {
  val `type`: String = "rate"
}

case class EvalDataSize(total: Int, details: Map[String, Int] = Map.empty[String, Int])
