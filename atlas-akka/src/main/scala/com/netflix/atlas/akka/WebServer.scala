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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

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

  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  private val port = config.getInt("atlas.akka.port")

  private val timeout = config.getDuration("atlas.akka.bind-timeout", TimeUnit.MILLISECONDS)
  private implicit val bindTimeout = Timeout(timeout, TimeUnit.MILLISECONDS)

  private var bindingFuture: Future[ServerBinding] = _

  protected def startImpl(): Unit = {
    val handler = new RequestHandler(config, classFactory)
    bindingFuture = Http().bindAndHandle(Route.handlerFlow(handler.routes), "0.0.0.0", port)
    logger.info(s"started $name on port $port")
  }

  protected def stopImpl(): Unit = {
    bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
  }

  def actorSystem: ActorSystem = system
}
