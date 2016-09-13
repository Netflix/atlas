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

  def port: Int = config.getInt("main.port")

  def defaultFrequency: Long = config.getLong("register.default-frequency")

  def redisHost: String = config.getString("redis.host")
  def redisPort: Int = config.getInt("redis.port")
  def redisTTL: Int = config.getInt("redis.ttl")
  def redisKeyPrefix: String = config.getString("redis.expr-key-prefix")

  def excludedWords: Set[String] = {
    import scala.collection.JavaConversions._
    config.getStringList("expr.complete.excluded-words").toSet
  }
}
