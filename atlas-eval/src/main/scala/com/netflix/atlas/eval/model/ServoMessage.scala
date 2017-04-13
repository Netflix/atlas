/*
 * Copyright 2014-2017 Netflix, Inc.
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

import com.netflix.atlas.core.model.Datapoint

/**
  * Message from legacy mantis source integrated with base-server.
  *
  * @param metrics
  *     Datapoints that matched the query.
  */
case class ServoMessage(metrics: List[ServoDatapoint])

/**
  * Datapoint from the servo source. The timestamps may or may not already be normalized
  * to the step boundary. Note, the step size is fixed based on the `servo.pollers`
  * setting of the plugin and cannot be negotiated as part of the subscription.
  *
  * @param config
  *     Pair with the name and other tags.
  * @param timestamp
  *     Timestamp for the datapoint. Depending on client settings it may or may not be
  *     normalized to the step boundary.
  * @param value
  *     Value for the datapoint.
  */
case class ServoDatapoint(config: ServoConfig, timestamp: Long, value: Double) {
  def toDatapoint(step: Long): Datapoint = {
    val boundary = if (timestamp % step == 0) timestamp else timestamp / step * step
    Datapoint(config.tags + ("name" -> config.name), boundary, value)
  }
}

/**
  * Config used to identify a servo metric. See servo's MonitorConfig for more information.
  *
  * @param name
  *     Name used to describe the measurement.
  * @param tags
  *     Other dimensions for drilling into the data.
  */
case class ServoConfig(name: String, tags: Map[String, String])
