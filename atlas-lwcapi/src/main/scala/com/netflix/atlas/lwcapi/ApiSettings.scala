/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import com.netflix.atlas.config.ConfigManager
import com.typesafe.config.Config

object ApiSettings extends ApiSettings(ConfigManager.current)

class ApiSettings(root: => Config) {
  private def config = root.getConfig("atlas.lwcapi")

  def defaultFrequency: Long = config.getDuration("register.default-frequency").toMillis

  def redisHost: String = config.getString("redis.host")
  def redisPort: Int = config.getInt("redis.port")
  def redisTTL: Long = config.getDuration("redis.ttl").toMillis
  def redisPrefix: String = {
    if (config.hasPath("redis.prefix")) {
      if (config.getString("redis.prefix").nonEmpty)
        config.getString("redis.prefix") + "."
      else
        ""
    } else ""
  }

  def redisPrefixFor(suffix: String): String = {
    s"$redisPrefix$suffix"
  }

  def stripRedisPrefix(s: String): String = {
    if (redisPrefix.isEmpty) s else s.stripPrefix(redisPrefix)
  }
}
