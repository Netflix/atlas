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

sealed trait ValidationResult {

  def isSuccess: Boolean

  def isFailure: Boolean = !isSuccess
}

object ValidationResult {

  /** Indicates the tag set passed validation with no issues. */
  case object Pass extends ValidationResult {

    def isSuccess: Boolean = true
  }

  /**
    * Indicates the tag set violates at least one of the rules.
    *
    * @param rule
    *     Name of the rule that failed.
    * @param reason
    *     Description of the failure.
    * @param tags
    *     The set of tags that was checked. This is used to help provide context
    *     to the user when the failure is displayed.
    */
  case class Fail(rule: String, reason: String, tags: Map[String, String] = Map.empty)
      extends ValidationResult {

    def isSuccess: Boolean = false
  }
}
