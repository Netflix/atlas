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

import java.net.BindException

import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.Tcp
import akka.pattern.ask
import akka.io.IO
import akka.io.Inet
import akka.util.Timeout
import com.netflix.iep.service.AbstractService
import com.typesafe.scalalogging.StrictLogging
import spray.can.Http

import scala.concurrent.Await
import scala.util.Failure
import scala.util.Success


class WebServer(name: String, port: Int) extends AbstractService with StrictLogging {

  import scala.concurrent.duration._
  private implicit val bindTimeout = Timeout(1.second)

  private implicit var system: ActorSystem = null

  protected def configure(): Unit = {
  }

  protected def startImpl(): Unit = {
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
    Await.ready(IO(Http).ask(bind), Duration.Inf).value.get match {
      case Success(Tcp.CommandFailed(_)) =>
        logger.error("server failed to start")
        throw new BindException(s"could not bind: $bind")
      case Success(b) =>
        logger.info(s"server started on port $port")
        handler.tell(b, handler)
      case Failure(t) =>
        logger.error("server failed to start", t)
        throw t;
    }
  }

  protected def stopImpl(): Unit = {
    system.shutdown()
    system = null
  }

  def actorSystem: ActorSystem = system
}
