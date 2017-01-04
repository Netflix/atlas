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
package com.netflix.atlas.core.validation

import com.typesafe.config.Config

/**
 * Verifies that the keys and values only use alpha-numeric, underscore, dash, and period.
 */
class ValidCharactersRule(config: Config) extends TagRule {

  import ValidCharactersRule._

  def validate(k: String, v: String): ValidationResult = {
    var i = 0
    var length = k.length
    while (i < length) {
      if (!isSupported(k.charAt(i))) {
        return failure(s"invalid characters in key: [$k] ([-_.A-Za-z0-9] are allowed)")
      }
      i += 1
    }

    i = 0
    length = v.length
    while (i < length) {
      if (!isSupported(v.charAt(i))) {
        return failure(s"invalid characters in value: $k = [$v] ([-_.A-Za-z0-9] are allowed)")
      }
      i += 1
    }

    ValidationResult.Pass
  }
}

object ValidCharactersRule {
  private final val supported = {
    val cs = new Array[Boolean](128)
    (0   until 128).foreach { i => cs(i) = false }
    ('A' to    'Z').foreach { c => cs(c) = true }
    ('a' to    'z').foreach { c => cs(c) = true }
    ('0' to    '9').foreach { c => cs(c) = true }
    cs('-') = true
    cs('_') = true
    cs('.') = true
    cs
  }

  private final def isSupported(c: Char): Boolean = {
    c >=0 && c < 128 && supported(c)
  }
}
