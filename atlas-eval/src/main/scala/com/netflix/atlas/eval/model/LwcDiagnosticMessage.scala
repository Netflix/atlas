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
package com.netflix.atlas.eval.model

import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage

/**
  * Diagnostic message for a particular expression.
  *
  * @param id
  *     Identifies the expression that resulted in this message being generated.
  * @param message
  *     Actual message to pass through to the user.
  */
case class LwcDiagnosticMessage(id: String, message: DiagnosticMessage) extends JsonSupport {
  val `type`: String = "diagnostic"
}

object LwcDiagnosticMessage {

  def info(id: String, message: String): LwcDiagnosticMessage = {
    apply(id, DiagnosticMessage.info(message))
  }

  def error(id: String, message: String): LwcDiagnosticMessage = {
    apply(id, DiagnosticMessage.error(message))
  }
}
