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
package com.netflix.atlas.pekko

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

import org.apache.pekko.actor.ActorPath
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.dispatch.Envelope
import org.apache.pekko.dispatch.MailboxType
import org.apache.pekko.dispatch.MessageQueue
import org.apache.pekko.dispatch.ProducesMessageQueue
import org.apache.pekko.dispatch.UnboundedMessageQueueSemantics
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config

object UnboundedMeteredMailbox {

  private case class Entry(v: Envelope, t: Long = System.nanoTime)

  class MeteredMessageQueue(path: String) extends MessageQueue with UnboundedMessageQueueSemantics {

    private final val queue = new ConcurrentLinkedQueue[Entry]

    private val registry = Spectator.globalRegistry()
    private val insertCounter = registry.counter("pekko.queue.insert", "path", path)
    private val waitTimer = registry.timer("pekko.queue.wait", "path", path)

    PolledMeter
      .using(registry)
      .withName("pekko.queue.size")
      .withTag("path", path)
      .monitorSize(queue)

    def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      insertCounter.increment()
      queue.offer(Entry(handle))
    }

    def dequeue(): Envelope = {
      val tmp = queue.poll()
      if (tmp == null) null
      else {
        val dur = System.nanoTime - tmp.t
        waitTimer.record(dur, TimeUnit.NANOSECONDS)
        tmp.v
      }
    }

    def numberOfMessages: Int = queue.size

    def hasMessages: Boolean = !queue.isEmpty

    def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      queue.clear()
    }
  }
}

class UnboundedMeteredMailbox(
  @scala.annotation.nowarn settings: ActorSystem.Settings,
  config: Config
) extends MailboxType
    with ProducesMessageQueue[UnboundedMeteredMailbox.MeteredMessageQueue] {

  import UnboundedMeteredMailbox.*

  private val Path = config.getString("path-pattern").r

  /** Summarizes a path for use in a metric tag. */
  def tagValue(path: ActorPath): String = {
    path.toString match {
      case Path(v) => v
      case _       => "uncategorized"
    }
  }

  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    val path = owner.fold("unknown")(r => tagValue(r.path))
    new MeteredMessageQueue(path)
  }
}
