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
package com.netflix.atlas.poller

import com.netflix.atlas.core.model.Datapoint

/**
  * Messages used for polling actors.
  */
object Messages {

  /**
    * Sent by the ClientActor to the sender to indicate that the message was sent out. This
    * can be disabled using the `atlas.poller.sink.send-ack` property.
    */
  case object Ack

  /**
    * Sent by the PollerManager to pollers to request data. Pollers should respond with
    * a MetricsPayload.
    */
  case object Tick

  /**
    * Metrics payload that pollers will send back to the manager.
    *
    * @param tags
    *     Common tags that should get added to all metrics in the payload.
    * @param metrics
    *     Metrics collected by the poller.
    */
  case class MetricsPayload(tags: Map[String, String] = Map.empty, metrics: List[Datapoint] = Nil)

  /**
    * Represents a failure response message from the publish endpoint.
    *
    * @param `type`
    *     Message type. Should always be "error".
    * @param errorCount
    *     Number of datapoints that failed validation.
    * @param message
    *     Reasons for why datapoints were dropped.
    */
  case class FailureResponse(`type`: String, errorCount: Int, message: List[String])
}
