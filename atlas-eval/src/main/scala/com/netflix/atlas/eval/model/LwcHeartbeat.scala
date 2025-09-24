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

/**
  * Heartbeat message to indicate the time according to the server side. The eval client always
  * adjusts to the timestamps of the data that is flowing through it. These messages can be used
  * by the server side to ensure that there will always be at least one message for a given step
  * size that is flowing through to the client so it will close out and evaluate a given time
  * interval.
  *
  * @param timestamp
  *     Current time aligned to the last completed step boundary.
  * @param step
  *     Step size for this heartbeat message. The server will typically send one heartbeat
  *     foreach step size used in a subscription.
  */
case class LwcHeartbeat(timestamp: Long, step: Long) extends JsonSupport {

  val `type`: String = "heartbeat"

  require(timestamp % step == 0, s"timestamp ($timestamp) must be on boundary of step ($step)")
}
