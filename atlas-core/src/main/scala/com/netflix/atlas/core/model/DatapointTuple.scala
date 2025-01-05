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
package com.netflix.atlas.core.model

/**
  * Simple tuple representing a datapoint. Can be used in place of [[Datapoint]] when
  * the use-case does not require a [[TimeSeries]]. The id must be pre-computed for the
  * tuple where it cannot be passed in an will be computed on first access for [[Datapoint]].
  */
case class DatapointTuple(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double) {

  def toDatapoint: Datapoint = {
    Datapoint(tags, timestamp, value)
  }
}
