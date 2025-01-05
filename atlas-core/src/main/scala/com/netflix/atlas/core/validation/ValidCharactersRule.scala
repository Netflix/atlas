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
package com.netflix.atlas.core.validation

import com.netflix.spectator.impl.AsciiSet
import com.typesafe.config.Config

/**
  * Verifies that the keys and values only use the set of ASCII characters specificied
  * in the config. By default it will be alpha-numeric, underscore, dash, and period.
  * Sample config:
  *
  * ```
  * default-pattern = "-._A-Za-z0-9"
  * overrides = [
  *   {
  *     key = "nf.cluster"
  *     value = "-._A-Za-z0-9^~"
  *   },
  *   {
  *     key = "nf.asg"
  *     value = "-._A-Za-z0-9^~"
  *   }
  * ]
  * ```
  */
case class ValidCharactersRule(defaultSet: AsciiSet, overrides: Map[String, AsciiSet])
    extends TagRule {

  def validate(k: String, v: String): String = {
    if (!defaultSet.containsAll(k)) {
      return s"invalid characters in key: [$k] ([$defaultSet] are allowed)"
    }

    val valueSet = overrides(k)
    if (!valueSet.containsAll(v)) {
      return s"invalid characters in value: $k = [$v] ([$valueSet] are allowed)"
    }

    TagRule.Pass
  }
}

object ValidCharactersRule {

  def apply(config: Config): ValidCharactersRule = {
    import scala.jdk.CollectionConverters.*

    def loadOverrides: Map[String, AsciiSet] = {
      val entries = config.getConfigList("overrides").asScala.map { cfg =>
        val k = cfg.getString("key")
        val v = AsciiSet.fromPattern(cfg.getString("value"))
        k -> v
      }
      entries.toMap
    }

    val defaultSet = {
      if (config.hasPath("default-pattern"))
        AsciiSet.fromPattern(config.getString("default-pattern"))
      else
        AsciiSet.fromPattern("-._A-Za-z0-9")
    }

    val overrides = {
      val m = if (config.hasPath("overrides")) loadOverrides else Map.empty[String, AsciiSet]
      m.withDefaultValue(defaultSet)
    }

    new ValidCharactersRule(defaultSet, overrides)
  }
}
