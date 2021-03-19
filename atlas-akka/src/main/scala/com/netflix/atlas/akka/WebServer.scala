/*
 * Copyright 2014-2021 Netflix, Inc.
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

import javax.inject.Inject
import javax.inject.Singleton
import akka.actor.ActorSystem
import akka.http.scaladsl.ConnectionContext
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.HttpsConnectionContext
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import com.typesafe.sslconfig.ssl.ClientAuth
import com.typesafe.sslconfig.ssl.ConfigSSLContextBuilder
import com.typesafe.sslconfig.ssl.DefaultKeyManagerFactoryWrapper
import com.typesafe.sslconfig.ssl.DefaultTrustManagerFactoryWrapper
import com.typesafe.sslconfig.ssl.SSLConfigFactory
import com.typesafe.sslconfig.ssl.SSLConfigSettings
import com.typesafe.sslconfig.util.LoggerFactory
import com.typesafe.sslconfig.util.NoDepsLogger
import org.slf4j.Logger

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

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
  implicit val system: ActorSystem,
  implicit val materializer: Materializer
) extends AbstractService
    with StrictLogging {

  private implicit val executionContext: ExecutionContext = system.dispatcher

  private val portConfigs = WebServer.getPortConfigs(config, "atlas.akka.ports")

  private var bindingFutures: List[Future[ServerBinding]] = Nil

  protected def startImpl(): Unit = {
    val handler = new RequestHandler(config, classFactory)
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
    sslConfigOption: Option[SSLConfigSettings]
  ) {
    require(!secure || sslConfigOption.isDefined, s"ssl-config is not set for secure port $port")

    private val sslContext = sslConfigOption.map { sslConfig =>
      val keyManager = new DefaultKeyManagerFactoryWrapper(sslConfig.keyManagerConfig.algorithm)
      val trustManager = new DefaultTrustManagerFactoryWrapper(
        sslConfig.trustManagerConfig.algorithm
      )
      new ConfigSSLContextBuilder(SslLoggerFactory, sslConfig, keyManager, trustManager).build()
    }.orNull

    private val clientAuth = sslConfigOption.map(_.sslParametersConfig.clientAuth).orNull

    def createConnectionContext: HttpsConnectionContext = {
      ConnectionContext.httpsServer { () =>
        val sslEngine = sslContext.createSSLEngine()
        sslEngine.setUseClientMode(false)
        clientAuth match {
          case ClientAuth.Default => sslEngine.setNeedClientAuth(true)
          case ClientAuth.None    =>
          case ClientAuth.Need    => sslEngine.setNeedClientAuth(true)
          case ClientAuth.Want    => sslEngine.setWantClientAuth(true)
        }
        sslEngine
      }
    }
  }

  private def getPortConfigs(config: Config, path: String): List[PortConfig] = {
    import scala.jdk.CollectionConverters._
    config
      .getConfigList(path)
      .asScala
      .map(toPortConfig)
      .toList
  }

  private def toPortConfig(config: Config): PortConfig = {
    PortConfig(
      config.getInt("port"),
      config.getBoolean("secure"),
      if (config.hasPath("ssl-config"))
        Some(SSLConfigFactory.parse(config.getConfig("ssl-config")))
      else
        None
    )
  }

  /**
    * The sslconfig library has its own logging interface to avoid dependencies. Map it
    * to slf4j.
    */
  private object SslLoggerFactory extends LoggerFactory {

    override def apply(clazz: Class[_]): NoDepsLogger = {
      apply(clazz.getName)
    }

    override def apply(name: String): NoDepsLogger = {
      val logger = org.slf4j.LoggerFactory.getLogger(name)
      new SslLogger(logger)
    }
  }

  private class SslLogger(logger: Logger) extends NoDepsLogger {

    override def isDebugEnabled: Boolean = logger.isDebugEnabled

    override def debug(msg: String): Unit = logger.debug(msg)

    override def info(msg: String): Unit = logger.info(msg)

    override def warn(msg: String): Unit = logger.warn(msg)

    override def error(msg: String): Unit = logger.error(msg)

    override def error(msg: String, throwable: Throwable): Unit = logger.error(msg, throwable)
  }
}
