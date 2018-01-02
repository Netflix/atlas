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
package com.netflix.atlas.akka

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.util.Timeout
import com.netflix.iep.service.DefaultClassFactory
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

import scala.concurrent.Await

class ActorServiceSuite extends FunSuite {

  import scala.concurrent.duration._
  implicit val timeout = Timeout(5.seconds)

  test("simple actor") {
    val config = ConfigFactory.parseString(
      s"""
        |atlas.akka.actors = [
        |  {
        |    name = test
        |    class = "${classOf[ActorServiceSuite.EchoActor].getName}"
        |  }
        |]
      """.stripMargin)
    val system = ActorSystem("test", config)
    val service = new ActorService(system, config, new DefaultClassFactory())
    service.start()

    try {
      val ref = system.actorSelection("/user/test")
      val v = Await.result(akka.pattern.ask(ref, "ping"), Duration.Inf)
      assert(v === "ping")
    } finally {
      service.stop()
      Await.ready(system.terminate(), Duration.Inf)
    }
  }

  test("actor with router config") {
    val config = ConfigFactory.parseString(
      s"""
         |atlas.akka.actors = [
         |  {
         |    name = test
         |    class = "${classOf[ActorServiceSuite.EchoActor].getName}"
         |  }
         |]
         |
         |akka.actor.deployment {
         |  /test {
         |    router = round-robin-pool
         |    nr-of-instances = 2
         |  }
         |}
      """.stripMargin)
    val system = ActorSystem("test", config)
    val service = new ActorService(system, config, new DefaultClassFactory())
    service.start()

    try {
      val ref = system.actorSelection("/user/test")
      val v = Await.result(akka.pattern.ask(ref, "ping"), Duration.Inf)
      assert(v === "ping")
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
