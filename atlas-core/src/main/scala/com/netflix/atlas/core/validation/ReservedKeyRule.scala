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

import com.typesafe.config.Config

import java.util

/**
  * Verifies that only allowed keys are used for reserved prefixes. Reserved prefixes are used
  * to prevent user defined tags from overlapping with common infrastructure tagging that should
  * be consistent for all data. Sample config:
  *
  * ```
  * prefix = "nf."
  * allowed-keys = ["app", "cluster"]
  * ```
  *
  * This config would only allow "nf.app" and "nf.cluster" with a prefix of "nf.".
  */
case class ReservedKeyRule(prefix: String, allowedKeys: Set[String]) extends TagRule {

  import scala.jdk.CollectionConverters.*

  // Used for contains check as it testing shows it to have better performance
  private val hashSet = new util.HashSet[String](allowedKeys.asJava)

  override def validate(k: String, v: String): String = {
    if (k.startsWith(prefix) && !hashSet.contains(k))
      s"invalid key for reserved prefix '$prefix': $k"
    else
      TagRule.Pass
  }
}

object ReservedKeyRule {

  def apply(config: Config): ReservedKeyRule = {
    import scala.jdk.CollectionConverters.*

    val prefix = config.getString("prefix")
    val allowedKeys =
      config.getStringList("allowed-keys").asScala.map(k => s"$prefix$k").toSet

    new ReservedKeyRule(prefix, allowedKeys)
  }
}
