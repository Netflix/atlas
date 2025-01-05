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

import org.apache.pekko.http.scaladsl.ConnectionContext
import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import com.typesafe.config.Config
import com.typesafe.sslconfig.ssl.ClientAuth
import com.typesafe.sslconfig.ssl.ConfigSSLContextBuilder
import com.typesafe.sslconfig.ssl.DefaultKeyManagerFactoryWrapper
import com.typesafe.sslconfig.ssl.DefaultTrustManagerFactoryWrapper
import com.typesafe.sslconfig.ssl.SSLConfigFactory
import com.typesafe.sslconfig.util.LoggerFactory
import com.typesafe.sslconfig.util.NoDepsLogger
import org.slf4j.Logger

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine

/**
  * Context factory based on ssl-config library.
  *
  * @param config
  *     See https://lightbend.github.io/ssl-config/KeyStores.html.
  */
class ConfigConnectionContextFactory(config: Config) extends ConnectionContextFactory {

  import ConfigConnectionContextFactory.*

  private val sslConfig = SSLConfigFactory.parse(config)

  override val sslContext: SSLContext = {
    val keyManager = new DefaultKeyManagerFactoryWrapper(sslConfig.keyManagerConfig.algorithm)
    val trustManager = new DefaultTrustManagerFactoryWrapper(
      sslConfig.trustManagerConfig.algorithm
    )
    new ConfigSSLContextBuilder(SslLoggerFactory, sslConfig, keyManager, trustManager).build()
  }

  override def sslEngine: SSLEngine = {
    val sslEngine = sslContext.createSSLEngine()
    sslEngine.setUseClientMode(false)
    sslConfig.sslParametersConfig.clientAuth match {
      case ClientAuth.Default => sslEngine.setNeedClientAuth(true)
      case ClientAuth.None    =>
      case ClientAuth.Need    => sslEngine.setNeedClientAuth(true)
      case ClientAuth.Want    => sslEngine.setWantClientAuth(true)
    }
    sslEngine
  }

  override def httpsConnectionContext: HttpsConnectionContext = {
    ConnectionContext.httpsServer(() => sslEngine)
  }
}

object ConfigConnectionContextFactory {

  /**
    * The sslconfig library has its own logging interface to avoid dependencies. Map it
    * to slf4j.
    */
  private object SslLoggerFactory extends LoggerFactory {

    override def apply(clazz: Class[?]): NoDepsLogger = {
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
