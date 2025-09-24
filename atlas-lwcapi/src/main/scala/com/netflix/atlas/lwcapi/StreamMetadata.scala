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
package com.netflix.atlas.lwcapi

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Clock
import com.netflix.spectator.impl.StepLong

/**
  * Metadata for a stream.
  *
  * @param streamId
  *     Unique id for a stream specified by the caller. Used to route data and detect
  *     reconnections.
  * @param remoteAddress
  *     IP address of the remote consumer. Only used to help with debugging.
  * @param receivedMessages
  *     Number of messages that were successfully received. Currently this is step based
  *     and will track messages received in the last minute.
  * @param droppedMessages
  *     Number of messages that were dropped because the queue was full. Currently this is step
  *     based and will track messages dropped in the last minute.
  */
case class StreamMetadata(
  streamId: String,
  remoteAddress: String = "unknown",
  clock: Clock = Clock.SYSTEM,
  receivedMessages: StepLong = new StepLong(0, Clock.SYSTEM, 60_000),
  droppedMessages: StepLong = new StepLong(0, Clock.SYSTEM, 60_000)
) extends JsonSupport {

  def updateReceived(n: Int): Unit = {
    receivedMessages.addAndGet(clock.wallTime(), n)
  }

  def updateDropped(n: Int): Unit = {
    droppedMessages.addAndGet(clock.wallTime(), n)
  }

  override def hasCustomEncoding: Boolean = true

  override def encode(gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeStringField("streamId", streamId)
    gen.writeStringField("remoteAddress", remoteAddress)
    gen.writeNumberField("receivedMessages", receivedMessages.poll())
    gen.writeNumberField("droppedMessages", droppedMessages.poll())
    gen.writeEndObject()
  }
}
