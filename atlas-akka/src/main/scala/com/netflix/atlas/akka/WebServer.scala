/*
 * Copyright 2015 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Inet
import com.typesafe.scalalogging.StrictLogging
import spray.can.Http


class WebServer(name: String) extends StrictLogging {

  private implicit var system: ActorSystem = null

  protected def configure(): Unit = {
  }

  def start(port: Int) {
    assert(system == null, "actor system has already been started")
    logger.info(s"starting $name on port $port")
    system = ActorSystem(name)
    configure()
    val handler = system.actorOf(Props[RequestHandlerActor], "request-handler")
    val options = List(Inet.SO.ReuseAddress(true))
    val bind = Http.Bind(handler,
      interface = "0.0.0.0",
      port = port,
      backlog = 2048,
      options = options)
    IO(Http).tell(bind, handler)
  }

  def shutdown() {
    system.shutdown()
    system = null
  }

  def actorSystem: ActorSystem = system
}
