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
package com.netflix.atlas.aws

import java.util.concurrent.TimeUnit

import com.amazonaws.AmazonWebServiceClient
import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.netflix.atlas.config.ConfigManager
import com.typesafe.config.Config


/**
 * Helper for creating Amazon client objects based on the current configuration.
 */
object AwsClientFactory {

  private val defaultConfig = ConfigManager.current.getConfig("atlas.aws")

  def default: AwsClientFactory = {
    new DefaultAwsClientFactory(new DefaultAWSCredentialsProviderChain, defaultConfig)
  }

  val defaultClientConfig: ClientConfiguration = {
    createClientConfig(defaultConfig.getConfig("client"))
  }

  def createClientConfig(implicit config: Config): ClientConfiguration = {
    val settings = new ClientConfiguration

    // Should be the default, but just to make it explicit
    settings.setProtocol(Protocol.HTTPS)

    // Typically use the defaults
    setIfPresent("useGzip",           config.getBoolean, settings.setUseGzip)
    setIfPresent("useReaper",         config.getBoolean, settings.setUseReaper)
    setIfPresent("useTcpKeepAlive",   config.getBoolean, settings.setUseTcpKeepAlive)
    setIfPresent("maxConnections",    config.getInt,     settings.setMaxConnections)
    setIfPresent("maxErrorRetry",     config.getInt,     settings.setMaxErrorRetry)
    setIfPresent("connectionTTL",     getMillis,         settings.setConnectionTTL)
    setIfPresent("connectionTimeout", getTimeout,        settings.setConnectionTimeout)
    setIfPresent("socketTimeout",     getTimeout,        settings.setSocketTimeout)
    setIfPresent("userAgent",         config.getString,  settings.setUserAgent)
    setIfPresent("proxyPort",         config.getInt,     settings.setProxyPort)
    setIfPresent("proxyHost",         config.getString,  settings.setProxyHost)
    setIfPresent("proxyDomain",       config.getString,  settings.setProxyDomain)
    setIfPresent("proxyWorkstation",  config.getString,  settings.setProxyWorkstation)
    setIfPresent("proxyUsername",     config.getString,  settings.setProxyUsername)
    setIfPresent("proxyPassword",     config.getString,  settings.setProxyPassword)
    settings
  }

  private def getMillis(key: String)(implicit config: Config): Long = {
    config.getDuration(key, TimeUnit.MILLISECONDS)
  }

  private def getTimeout(key: String)(implicit config: Config): Int = getMillis(key).toInt

  private def setIfPresent[T](key: String, g: String => T, s: T => Unit)(implicit config: Config): Unit = {
    if (config.hasPath(key)) s(g(key))
  }
}

trait AwsClientFactory {
  def newInstance[T <: AmazonWebServiceClient](c: Class[T]): T
}
