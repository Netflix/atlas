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
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.DeadLetter
import org.apache.pekko.actor.PoisonPill
import org.apache.pekko.actor.Props
import org.apache.pekko.actor.SuppressedDeadLetter
import org.apache.pekko.testkit.ImplicitSender
import org.apache.pekko.testkit.TestActorRef
import org.apache.pekko.testkit.TestKitBase
import com.netflix.spectator.api.DefaultRegistry
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class DeadLetterStatsActorSuite extends FunSuite with TestKitBase with ImplicitSender {

  override implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  private val config = ConfigFactory.parseString(
    """
      |atlas.pekko.path-pattern = "^pekko://(?:[^/]+)/(?:system|user)/([^/]+)(?:/.*)?$"
    """.stripMargin
  )

  private val registry = new DefaultRegistry()
  private val ref = TestActorRef(new DeadLetterStatsActor(registry, config))

  private val sender = newRef("from")
  private val recipient = newRef("to")

  private def newRef(name: String): ActorRef = {
    val r = system.actorOf(
      Props(new Actor {

        override def receive: Receive = {
          case _ =>
        }
      }),
      name
    )
    system.stop(r)
    r
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  test("DeadLetter") {
    val id = registry
      .createId("pekko.deadLetters")
      .withTag("class", "DeadLetter")
      .withTag("sender", "from")
      .withTag("recipient", "to")

    assertEquals(registry.counter(id).count(), 0L)
    ref ! DeadLetter("foo", sender, recipient)
    assertEquals(registry.counter(id).count(), 1L)
  }

  test("SuppressedDeadLetter") {
    val id = registry
      .createId("pekko.deadLetters")
      .withTag("class", "SuppressedDeadLetter")
      .withTag("sender", "from")
      .withTag("recipient", "to")

    assertEquals(registry.counter(id).count(), 0L)
    ref ! SuppressedDeadLetter(PoisonPill, sender, recipient)
    assertEquals(registry.counter(id).count(), 1L)
  }
}
