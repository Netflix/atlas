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

import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import com.typesafe.config.Config

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine

/**
  * Base interface for setting up the connection context of an HTTPS server binding. A custom
  * implementation can be used for full control. Implementations must have a constructor that
  * takes a Config object or that is empty.
  */
trait ConnectionContextFactory {

  def sslContext: SSLContext

  def sslEngine: SSLEngine

  def httpsConnectionContext: HttpsConnectionContext
}

object ConnectionContextFactory {

  def apply(config: Config): ConnectionContextFactory = {
    val contextFactoryClass = {
      if (config.hasPath("context-factory")) {
        loadClass(config.getString("context-factory"))
      } else
        classOf[ConfigConnectionContextFactory]
    }

    try {
      // Look for constructor that takes a config object
      val ctor = contextFactoryClass.getConstructor(classOf[Config])
      ctor.newInstance(config.getConfig("ssl-config"))
    } catch {
      case _: NoSuchMethodException =>
        // Use an empty constructor
        val ctor = contextFactoryClass.getConstructor()
        ctor.newInstance()
    }
  }

  private def loadClass(className: String): Class[ConnectionContextFactory] = {
    Class.forName(className).asInstanceOf[Class[ConnectionContextFactory]]
  }
}
