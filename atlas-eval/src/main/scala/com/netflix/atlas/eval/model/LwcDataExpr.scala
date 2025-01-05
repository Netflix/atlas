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

import com.fasterxml.jackson.annotation.JsonAlias

/**
  * Triple representing the id, data expression, and frequency for a given data
  * flow. The id is needed to match [[LwcDatapoint]]s to the data expression they
  * were generated for.
  *
  * @param id
  *     Id used by LWC service for this expression. Data generated coming in will
  *     be tagged with this id.
  * @param expression
  *     Data expression that was used to generate the intermediate result on each
  *     client and that can be used for the final aggregation step during consumption.
  * @param step
  *     The step size used for this stream of data.
  */
case class LwcDataExpr(id: String, expression: String, @JsonAlias(Array("frequency")) step: Long) {}
