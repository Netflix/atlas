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
package com.netflix.atlas.config

import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite


class ConfigManagerSuite extends FunSuite {
  private val home = System.getProperty("user.home")

  test("init") {
    assert(ConfigManager.current === ConfigFactory.load())
  }

  test("home is set") {
    assert(home !== null)
    assert(ConfigManager.current.getString("user.home") === home)
  }

  test("update") {
    ConfigManager.update(ConfigFactory.parseString(s"user.home = foo$home"))
    assert(ConfigManager.current.getString("user.home") === s"foo$home")
  }

  test("update empty") {
    ConfigManager.update(ConfigFactory.empty())
    assert(ConfigManager.current.getString("user.home") === s"foo$home")
  }

  test("update with var") {
    ConfigManager.update(ConfigFactory.parseString(s"foo = $${user.home}"))
    assert(ConfigManager.current.getString("foo") === s"foo$home")
  }

  test("set") {
    ConfigManager.set(ConfigFactory.empty())
    intercept[ConfigException] { ConfigManager.current.getString("user.home") }
  }

  test("re-resolve with update") {
    ConfigManager.update(ConfigFactory.parseString(s"""
      test.foo = 1
      test.bar = $${test.foo}
      """))
    assert(ConfigManager.current.getInt("test.bar") === 1)

    ConfigManager.update(ConfigFactory.parseString("test.foo = 2"))
    assert(ConfigManager.current.getInt("test.bar") === 2)
  }
}

