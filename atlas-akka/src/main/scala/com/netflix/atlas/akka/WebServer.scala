/*
 * Copyright 2014-2017 Netflix, Inc.
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
import javax.inject.Inject
import javax.inject.Singleton

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Inet
import akka.routing.FromConfig
import akka.util.Timeout
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import spray.can.Http

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

/**
  * Web server instance.
  *
  * @param config
  *     System configuration used for main server settings. In particular `atlas.akka.port`
  *     is used for setting the server port.
  * @param classFactory
  *     Used to create instances of class names from the config file via the injector. The
  *     endpoints listed in `atlas.akka.endpoints` will be created using this factory.
  * @param registry
  *     Metrics registry for reporting server stats.
  * @param system
  *     Instance of the actor system.
  */
@Singleton
class WebServer @Inject() (
  config: Config,
  classFactory: ClassFactory,
  registry: Registry,
  implicit val system: ActorSystem)
    extends AbstractService with StrictLogging {

  import scala.concurrent.duration._

  private val port = config.getInt("atlas.akka.port")

  private val timeout = config.getDuration("atlas.akka.bind-timeout", TimeUnit.MILLISECONDS)
  private implicit val bindTimeout = Timeout(timeout, TimeUnit.MILLISECONDS)

  protected def startImpl(): Unit = {
    logger.info(s"starting $name on port $port")

    val handler = system.actorOf(newRequestHandler, "request-handler")

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
      case Success(Http.Bound(addr)) =>
        logger.info(s"server started on $addr")
      case Failure(t) =>
        logger.error("server failed to start", t)
        throw t;
    }
  }

  private def newRequestHandler: Props = {
    val props = Props(classFactory.newInstance[Actor](classOf[RequestHandlerActor]))
    val routeCfgPath = "akka.actor.deployment./request-handler.router"
    if (config.hasPath(routeCfgPath)) FromConfig.props(props) else props
  }

  protected def stopImpl(): Unit = {
  }

  def actorSystem: ActorSystem = system
}
