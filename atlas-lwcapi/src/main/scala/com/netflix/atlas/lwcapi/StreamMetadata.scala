/*
 * Copyright 2014-2021 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicLong

/**
  * Metadata for a stream.
  *
  * @param streamId
  *     Unique id for a stream specified by the caller. Used to route data and detect
  *     reconnections.
  * @param remoteAddress
  *     IP address of the remote consumer. Only used to help with debugging.
  * @param receivedMessages
  *     Number of messages that were successfully received.
  * @param droppedMessages
  *     Number of messages that were dropped because the queue was full.
  */
case class StreamMetadata(
  streamId: String,
  remoteAddress: String = "unknown",
  receivedMessages: AtomicLong = new AtomicLong(),
  droppedMessages: AtomicLong = new AtomicLong()
)
