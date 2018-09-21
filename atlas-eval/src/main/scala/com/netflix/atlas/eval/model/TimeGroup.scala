/*
 * Copyright 2014-2018 Netflix, Inc.
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
import com.netflix.atlas.core.model.DataExpr

/**
  * A group of values for the same timestamp. This type is typically created as the result
  * of using the [[com.netflix.atlas.eval.stream.TimeGrouped]] operator on the stream.
  *
  * The values map should always be non-empty and have datapoints for all entries. Empty
  * entries should be omitted.
  *
  * @param timestamp
  *     Timestamp that applies to all values within the group.
  * @param values
  *     Values associated with this time.
  */
case class TimeGroup(timestamp: Long, values: Map[DataExpr, List[AggrDatapoint]]) {

  /**
    * Extract the step size from the time group. It is not explicitly checked on construction
    * to avoid the overhead, but a group should always have a uniform step size for all
    * datapoints.
    */
  def step: Long = {
    values.values.find(_.nonEmpty).fold(-1L)(_.head.step)
  }
}
