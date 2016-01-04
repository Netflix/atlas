/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.akka

import akka.actor.Actor
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.DeadLetter
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.SuppressedDeadLetter
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import com.netflix.spectator.api.DefaultRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuiteLike
import spray.can.Http

import scala.concurrent.Promise


class DeadLetterStatsActorSuite extends TestKit(ActorSystem())
    with ImplicitSender
    with FunSuiteLike
    with BeforeAndAfterAll {

  private val pathPattern = "^akka://(?:[^/]+)/(?:system|user)/([^/]+)(?:/.*)?$".r

  private def pathMapper(path: ActorPath): String = {
    path.toString match {
      case pathPattern(p) => p
      case _              => "uncategorized"
    }
  }

  private val registry = new DefaultRegistry()
  private val bindPromise = Promise[Http.Bound]()
  private val ref = TestActorRef(new DeadLetterStatsActor(registry, pathMapper))

  private val sender = newRef("from")
  private val recipient = newRef("to")

  private def newRef(name: String): ActorRef = {
    val r = system.actorOf(Props(new Actor {
      override def receive: Receive = {
        case _ =>
      }
    }), name)
    system.stop(r)
    r
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  private def get(k: String): Long = {
    import scala.collection.JavaConversions._
    registry.get(registry.createId(k)).measure().head.value.toLong
  }

  test("DeadLetter") {
    val id = registry.createId("akka.deadLetters")
      .withTag("class", "DeadLetter")
      .withTag("sender", "from")
      .withTag("recipient", "to")

    assert(0 === registry.counter(id).count())
    ref ! DeadLetter("foo", sender, recipient)
    assert(1 === registry.counter(id).count())
  }

  test("SuppressedDeadLetter") {
    val id = registry.createId("akka.deadLetters")
      .withTag("class", "SuppressedDeadLetter")
      .withTag("sender", "from")
      .withTag("recipient", "to")

    assert(0 === registry.counter(id).count())
    ref ! SuppressedDeadLetter(PoisonPill, sender, recipient)
    assert(1 === registry.counter(id).count())
  }
}
