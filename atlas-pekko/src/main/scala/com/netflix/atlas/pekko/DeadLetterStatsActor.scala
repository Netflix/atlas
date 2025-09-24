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

import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.AllDeadLetters
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

/**
  * Update counter for dead letters in the actor system. The counter name is `pekko.deadLetters`
  * and has dimensions for:
  *
  *  - `class`: the type of dead letter. Value should be either DeadLetter or SuppressedDeadLetter.
  *  - `sender`: summary of path for sender. See Paths for more details.
  *  - `recipient`: summary of path for recipient. See Paths for more details.
  *
  * To use subscribe to the dead letters on the event stream:
  *
  * http://doc.pekko.io/docs/pekko/2.4.0/scala/event-bus.html#Dead_Letters
  *
  * @param registry
  *     Spectator registry to use for metrics.
  * @param config
  *     Config to use for creating the path mapper using the `atlas.pekko.path-pattern`
  *     This pattern maps an actor path to a tag value for the metric. This should be
  *     chosen to avoid parts of the path such as incrementing counters in the path of
  *     short lived actors.
  */
class DeadLetterStatsActor(registry: Registry, config: Config) extends Actor with StrictLogging {

  context.system.eventStream.subscribe(self, classOf[AllDeadLetters])

  private val pathMapper = Paths.createMapper(config.getConfig("atlas.pekko"))
  private val deadLetterId = registry.createId("pekko.deadLetters")

  def receive: Receive = {
    case letter: AllDeadLetters =>
      // format: off
      val id = deadLetterId.withTags(
        "class",     letter.getClass.getSimpleName,
        "sender",    pathMapper(letter.sender.path),
        "recipient", pathMapper(letter.recipient.path)
      )
      registry.counter(id).increment()
      // format: on
  }
}
