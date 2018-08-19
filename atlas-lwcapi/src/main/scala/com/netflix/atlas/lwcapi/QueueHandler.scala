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
package com.netflix.atlas.lwcapi

import akka.stream.QueueOfferResult
import akka.stream.scaladsl.SourceQueueWithComplete
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

/**
  * Message handler for use with the [SubscriptionManager].
  *
  * @param id
  *     Stream id for this handler. Used to provide context in the log messages and easily
  *     be able to grep for a given id.
  * @param queue
  *     Underlying queue that will receive the messsages.
  */
class QueueHandler(id: String, queue: SourceQueueWithComplete[SSERenderable])
    extends StrictLogging {

  def offer(msg: SSERenderable): Future[QueueOfferResult] = {
    logger.trace(s"enqueuing message for $id: ${msg.toSSE}")
    queue.offer(msg)
  }

  def complete(): Unit = {
    logger.debug(s"queue complete for $id")
    queue.complete()
  }

  override def toString: String = s"QueueHandler($id)"
}
