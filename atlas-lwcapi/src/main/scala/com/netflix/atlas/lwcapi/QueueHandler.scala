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

import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.StreamOps
import com.typesafe.scalalogging.StrictLogging

/**
  * Message handler for use with the [SubscriptionManager].
  *
  * @param streamMeta
  *     Stream metadata for this handler. Used to provide context in the log messages and easily
  *     be able to grep for a given id.
  * @param queue
  *     Underlying queue that will receive the messsages.
  */
class QueueHandler(
  streamMeta: StreamMetadata,
  queue: StreamOps.BlockingSourceQueue[Seq[JsonSupport]]
) extends StrictLogging {

  private val id = streamMeta.streamId

  private def toJson(msgs: Seq[JsonSupport]): String = {
    msgs.map(_.toJson).mkString("[", ",", "]")
  }

  def offer(msgs: Seq[JsonSupport]): Unit = {
    logger.trace(s"enqueuing message for $id: ${toJson(msgs)}")
    if (!queue.offer(msgs)) {
      logger.debug(s"failed to enqueue message for $id: ${toJson(msgs)}")
      streamMeta.updateDropped(msgs.size)
    } else {
      streamMeta.updateReceived(msgs.size)
    }
  }

  def complete(): Unit = {
    if (queue.isOpen) {
      logger.debug(s"queue complete for $id")
      try {
        queue.complete()
      } catch {
        // Thrown if the queue is already closed. Can happen when a websocket
        // connects but doesn't send a message. See SubscribeApi#register
        case _: IllegalStateException => // no-op
      }
    }
  }

  override def toString: String = s"QueueHandler($id)"
}
