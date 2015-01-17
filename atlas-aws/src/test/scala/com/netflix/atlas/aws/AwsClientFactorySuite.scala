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

import com.amazonaws.ClientConfiguration
import com.netflix.atlas.config.ConfigManager
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class AwsClientFactorySuite extends FunSuite {

  val config = ConfigManager.current

  test("client config") {
    val awsDflt = new ClientConfiguration
    assert(!awsDflt.useGzip)

    val ourDflt = AwsClientFactory.defaultClientConfig
    assert(ourDflt.useGzip)
  }

  // Boolean flags
  List("Gzip", "Reaper", "TcpKeepAlive").foreach { name =>
    test(s"set true: $name") {
      val cfg = ConfigFactory.parseString(s"use$name = true")
      val clientConfig = AwsClientFactory.createClientConfig(cfg)
      assert(get[Boolean](clientConfig, s"use$name"))
    }

    test(s"set false: $name") {
      val cfg = ConfigFactory.parseString(s"use$name = false")
      val clientConfig = AwsClientFactory.createClientConfig(cfg)
      assert(!get[Boolean](clientConfig, s"use$name"))
    }
  }

  // Timeouts
  List("Socket", "Connection").foreach { name =>
    test(s"set timeout: $name") {
      val cfg = ConfigFactory.parseString(s"${name.toLowerCase}Timeout = 42 minutes")
      val clientConfig = AwsClientFactory.createClientConfig(cfg)
      assert(get[Int](clientConfig, s"get${name}Timeout") === 42 * 60 * 1000)
    }
  }

  // TTL, for some reason timeouts are int, but ttl is long
  test(s"connectionTTL: not set") {
    val cfg = ConfigFactory.parseString(s"connectionTTL = -1")
    val clientConfig = AwsClientFactory.createClientConfig(cfg)
    assert(get[Long](clientConfig, s"getConnectionTTL") === -1)
  }

  test(s"connectionTTL: time value") {
    val cfg = ConfigFactory.parseString(s"connectionTTL = 42 minutes")
    val clientConfig = AwsClientFactory.createClientConfig(cfg)
    assert(get[Long](clientConfig, s"getConnectionTTL") === 42 * 60 * 1000)
  }

  // Integers
  List("MaxConnections", "MaxErrorRetry", "ProxyPort").foreach { name =>
    test(s"set int: $name") {
      val cfg = ConfigFactory.parseString(s"${lowerFirstChar(name)} = 42")
      val clientConfig = AwsClientFactory.createClientConfig(cfg)
      assert(get[Int](clientConfig, s"get$name") === 42)
    }
  }

  // Strings
  val proxyStrings = List("Host", "Domain", "Workstation", "Username", "Password")
  ("UserAgent" :: proxyStrings.map(n => s"Proxy$n")).foreach { name =>
    test(s"set string: $name") {
      val cfg = ConfigFactory.parseString(s"${lowerFirstChar(name)} = foo")
      val clientConfig = AwsClientFactory.createClientConfig(cfg)
      assert(get[String](clientConfig, s"get$name") === "foo")
    }
  }

  private def lowerFirstChar(s: String): String = {
    s.substring(0, 1).toLowerCase + s.substring(1)
  }

  private def get[T: Manifest](obj: AnyRef, method: String): T = {
    obj.getClass.getMethod(method).invoke(obj).asInstanceOf[T]
  }
}
