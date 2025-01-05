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
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.util.Timeout
import com.netflix.iep.service.DefaultClassFactory
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import scala.concurrent.Await

class ActorServiceSuite extends FunSuite {

  import scala.concurrent.duration.*

  private implicit val timeout: Timeout = Timeout(5.seconds)

  test("simple actor") {
    val config = ConfigFactory.parseString(s"""
        |atlas.pekko.actors = [
        |  {
        |    name = test
        |    class = "${classOf[ActorServiceSuite.EchoActor].getName}"
        |  }
        |]
      """.stripMargin)
    val system = ActorSystem("test", config)
    val service = new ActorService(system, config, new NoopRegistry, new DefaultClassFactory())
    service.start()

    try {
      val ref = system.actorSelection("/user/test")
      val v = Await.result(org.apache.pekko.pattern.ask(ref, "ping"), Duration.Inf)
      assertEquals(v, "ping")
    } finally {
      service.stop()
      Await.ready(system.terminate(), Duration.Inf)
    }
  }

  test("actor with router config") {
    val config = ConfigFactory.parseString(s"""
         |atlas.pekko.actors = [
         |  {
         |    name = test
         |    class = "${classOf[ActorServiceSuite.EchoActor].getName}"
         |  }
         |]
         |
         |pekko.actor.deployment {
         |  /test {
         |    router = round-robin-pool
         |    nr-of-instances = 2
         |  }
         |}
      """.stripMargin)
    val system = ActorSystem("test", config)
    val service = new ActorService(system, config, new NoopRegistry, new DefaultClassFactory())
    service.start()

    try {
      val ref = system.actorSelection("/user/test")
      val v = Await.result(org.apache.pekko.pattern.ask(ref, "ping"), Duration.Inf)
      assertEquals(v, "ping")
    } finally {
      service.stop()
      Await.ready(system.terminate(), Duration.Inf)
    }
  }
}

object ActorServiceSuite {

  class EchoActor extends Actor {

    override def receive: Receive = {
      case v => sender() ! v
    }
  }
}
