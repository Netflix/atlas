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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * Web server instance.
  *
  * @param config
  *     System configuration used for main server settings. In particular `atlas.pekko.port`
  *     is used for setting the server port.
  * @param classFactory
  *     Used to create instances of class names from the config file via the injector. The
  *     endpoints listed in `atlas.pekko.endpoints` will be created using this factory.
  * @param registry
  *     Metrics registry for reporting server stats.
  * @param system
  *     Instance of the actor system.
  */
class WebServer(
  config: Config,
  classFactory: ClassFactory,
  registry: Registry,
  implicit val system: ActorSystem,
  implicit val materializer: Materializer
) extends AbstractService
    with StrictLogging {

  private implicit val executionContext: ExecutionContext = system.dispatcher

  private val portConfigs = WebServer.getPortConfigs(config, "atlas.pekko.ports")

  private var bindingFutures: List[Future[ServerBinding]] = Nil

  protected def startImpl(): Unit = {
    val handler = new RequestHandler(config, registry, classFactory)
    val routes = Route.toFlow(handler.routes)
    bindingFutures = portConfigs.map { portConfig =>
      var builder = Http().newServerAt("0.0.0.0", portConfig.port)
      if (portConfig.secure) {
        builder = builder.enableHttps(portConfig.createConnectionContext)
      }
      val future = builder.bindFlow(routes)
      logger.info(s"started $name on port ${portConfig.port}")
      future
    }
  }

  protected def stopImpl(): Unit = {
    val shutdownFuture = Future.sequence(bindingFutures.map(_.flatMap(_.unbind())))
    Await.ready(shutdownFuture, Duration.Inf)
  }

  def actorSystem: ActorSystem = system
}

object WebServer {

  private case class PortConfig(
    port: Int,
    secure: Boolean,
    contextFactory: ConnectionContextFactory = null
  ) {

    require(!secure || contextFactory != null, s"context is not set for secure port $port")

    def createConnectionContext: HttpsConnectionContext = {
      contextFactory.httpsConnectionContext
    }
  }

  private def getPortConfigs(config: Config, path: String): List[PortConfig] = {
    import scala.jdk.CollectionConverters.*
    config
      .getConfigList(path)
      .asScala
      .map(toPortConfig)
      .toList
  }

  private def toPortConfig(config: Config): PortConfig = {
    val port = config.getInt("port")
    val secure = config.getBoolean("secure")
    if (secure) {
      val contextFactory = ConnectionContextFactory(config)
      PortConfig(port, secure, contextFactory)
    } else {
      PortConfig(port, secure)
    }
  }
}
