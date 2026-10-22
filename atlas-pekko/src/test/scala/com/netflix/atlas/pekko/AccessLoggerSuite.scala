/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.typesafe.config.ConfigFactory
import munit.FunSuite

class AccessLoggerSuite extends FunSuite {

  test("sensitive headers: default is empty") {
    assertEquals(AccessLogger.loadSensitiveHeaders(ConfigFactory.load()), Set.empty[String])
  }

  test("sensitive headers: names are lower cased") {
    val config = ConfigFactory.parseString(
      """atlas.pekko.access-log.sensitive-headers = ["X-Proxy-Auth", "authorization"]"""
    )
    assertEquals(AccessLogger.loadSensitiveHeaders(config), Set("x-proxy-auth", "authorization"))
  }

  test("sensitive headers: names are trimmed") {
    val config = ConfigFactory.parseString(
      """atlas.pekko.access-log.sensitive-headers = [" Authorization "]"""
    )
    assertEquals(AccessLogger.loadSensitiveHeaders(config), Set("authorization"))
  }

  test("sensitive headers: missing path") {
    assertEquals(AccessLogger.loadSensitiveHeaders(ConfigFactory.empty()), Set.empty[String])
  }
}
