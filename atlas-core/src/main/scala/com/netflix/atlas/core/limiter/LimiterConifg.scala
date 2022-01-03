/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.core.limiter

import scala.collection.immutable.ArraySeq

/**
  * The configuration for [[CardinalityLimiter]].
  *
  * @param prefixConfigs list of config with prefix key and associated limits
  * @param tagValueLimit max number of values per non prefix key
  */
case class LimiterConfig(prefixConfigs: ArraySeq[PrefixConfig], tagValueLimit: Int) {
  require(prefixConfigs.length > 0)

  /**
    * All the prefix keys.
    */
  val prefixKeys: ArraySeq[String] = prefixConfigs.map(_.key)

  /**
    * get PrefixConfig by level.
    */
  def getPrefixConfig(level: Int): PrefixConfig = {
    prefixConfigs(level)
  }

  /**
    * Check if a key is one of the prefix keys.
    */
  def isPrefixKey(key: String): Boolean = {
    // Linear array search, number of prefix keys is usually small
    prefixKeys.contains(key)
  }
}

/**
  * Configuration for a prefix key.
  * @param key name of the key
  * @param valueLimit max number of values for this tag key
  * @param totalLimit max number of items cross all values
  */
case class PrefixConfig(key: String, valueLimit: Int, totalLimit: Int)
