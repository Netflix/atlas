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
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine

class ConnectionContextFactorySuite extends FunSuite {

  import ConnectionContextFactorySuite.*

  test("factory not set") {
    val config = ConfigFactory.load()
    val contextFactory = ConnectionContextFactory(config)
    assert(contextFactory.isInstanceOf[ConfigConnectionContextFactory])
  }

  test("empty constructor") {
    val config = ConfigFactory.parseString(s"""
        |context-factory = "${classOf[EmptyConstructorFactory].getName}"
        |""".stripMargin)
    val contextFactory = ConnectionContextFactory(config)
    assert(contextFactory.isInstanceOf[EmptyConstructorFactory])
  }

  test("config constructor") {
    val config = ConfigFactory.parseString(s"""
         |context-factory = "${classOf[ConfigConstructorFactory].getName}"
         |ssl-config {
         |  correct-subconfig = true
         |}
         |""".stripMargin)
    val contextFactory = ConnectionContextFactory(config)
    assert(contextFactory.isInstanceOf[ConfigConstructorFactory])
  }
}

object ConnectionContextFactorySuite {

  class EmptyConstructorFactory extends ConnectionContextFactory {

    override def sslContext: SSLContext = null

    override def sslEngine: SSLEngine = null

    override def httpsConnectionContext: HttpsConnectionContext = null
  }

  class ConfigConstructorFactory(config: Config) extends ConnectionContextFactory {

    require(config.getBoolean("correct-subconfig"))

    override def sslContext: SSLContext = null

    override def sslEngine: SSLEngine = null

    override def httpsConnectionContext: HttpsConnectionContext = null
  }
}
