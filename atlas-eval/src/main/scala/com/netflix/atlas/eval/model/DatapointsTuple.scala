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

import com.netflix.atlas.eval.stream.Evaluator

/**
  * Pairs set of data points with other arbitrary messages to pass through to the
  * consumer.
  */
case class DatapointsTuple(
  data: List[AggrDatapoint],
  messages: List[Evaluator.MessageEnvelope] = Nil
) {

  def step: Long = {
    if (data.nonEmpty) data.head.step else 0L
  }

  def groupByStep: List[DatapointsTuple] = {
    val dps = data.groupBy(_.step).map(t => DatapointsTuple(t._2)).toList
    if (messages.nonEmpty)
      DatapointsTuple(Nil, messages) :: dps
    else
      dps
  }
}
