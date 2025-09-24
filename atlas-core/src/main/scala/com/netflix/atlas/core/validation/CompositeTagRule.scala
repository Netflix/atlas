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

/** Verifies that all of the tag rules match. */
case class CompositeTagRule(tagRules: List[TagRule]) extends TagRule {

  override def validate(k: String, v: String): String = {
    validate(tagRules, k, v)
  }

  @scala.annotation.tailrec
  private def validate(rules: List[TagRule], k: String, v: String): String = {
    if (rules.isEmpty) {
      TagRule.Pass
    } else {
      val r = rules.head
      val result = r.validate(k, v)
      if (result == TagRule.Pass)
        validate(rules.tail, k, v)
      else
        result
    }
  }
}
