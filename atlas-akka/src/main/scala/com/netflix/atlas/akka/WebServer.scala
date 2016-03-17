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

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.AllDeadLetters
import akka.actor.Props
import akka.io.IO
import akka.io.Inet
import akka.util.Timeout
import com.netflix.atlas.config.ConfigManager
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.netflix.iep.service.DefaultClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import spray.can.Http

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success


class WebServer(classFactory: ClassFactory, registry: Registry, name: String, port: Int)
    extends AbstractService with StrictLogging {

  @deprecated(message = "Use default constructor instead.", since = "2016-03-16")
  def this(registry: Registry, name: String, port: Int) =
    this(new DefaultClassFactory(), registry, name, port)

  import scala.concurrent.duration._

  private val config = ConfigManager.current
  private val timeout = config.getDuration("atlas.akka.bind-timeout", TimeUnit.MILLISECONDS)
  private implicit val bindTimeout = Timeout(timeout, TimeUnit.MILLISECONDS)

  private implicit var system: ActorSystem = null

  protected def configure(): Unit = {
    val ref = system.actorOf(
      Props(new DeadLetterStatsActor(registry, Paths.tagValue)), "deadLetterStats")
    system.eventStream.subscribe(ref, classOf[AllDeadLetters])
  }

  protected def startImpl(): Unit = {
    assert(system == null, "actor system has already been started")
    logger.info(s"starting $name on port $port")
    system = ActorSystem(name)
    configure()

    val handler = system.actorOf(
      Props(classFactory.newInstance[Actor](classOf[RequestHandlerActor])), "request-handler")

    val bindPromise = Promise[Http.Bound]()
    val stats = system.actorOf(Props(new ServerStatsActor(registry, bindPromise)))
    val options = List(Inet.SO.ReuseAddress(true))
    val bind = Http.Bind(handler,
      interface = "0.0.0.0",
      port = port,
      backlog = 2048,
      options = options)
    IO(Http).tell(bind, stats)
    Await.ready(bindPromise.future, Duration.Inf).value.get match {
      case Success(_) =>
        logger.info(s"server started on port $port")
      case Failure(t) =>
        logger.error("server failed to start", t)
        throw t;
    }
  }

  protected def stopImpl(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
    system = null
  }

  def actorSystem: ActorSystem = system
}
