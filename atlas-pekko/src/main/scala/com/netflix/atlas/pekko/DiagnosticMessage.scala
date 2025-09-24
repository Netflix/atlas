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
package com.netflix.atlas.pekko

import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCode
import com.netflix.atlas.json.JsonSupport

object DiagnosticMessage {

  final val Info: String = "info"
  final val Warning: String = "warn"
  final val Error: String = "error"
  final val Close: String = "close"

  def info(message: String): DiagnosticMessage = {
    DiagnosticMessage(Info, message, None)
  }

  def warning(message: String): DiagnosticMessage = {
    DiagnosticMessage(Warning, message, None)
  }

  def error(message: String): DiagnosticMessage = {
    DiagnosticMessage(Error, message, None)
  }

  def error(t: Throwable): DiagnosticMessage = {
    error(s"${t.getClass.getSimpleName}: ${t.getMessage}")
  }

  val close: DiagnosticMessage = {
    DiagnosticMessage(Close, "operation complete", None)
  }

  def error(status: StatusCode, t: Throwable): HttpResponse = {
    error(status, s"${t.getClass.getSimpleName}: ${t.getMessage}")
  }

  def error(status: StatusCode, msg: String): HttpResponse = {
    val errorMsg = DiagnosticMessage.error(msg)
    val entity = HttpEntity(MediaTypes.`application/json`, errorMsg.toJson)
    HttpResponse(status = status, entity = entity)
  }
}

case class DiagnosticMessage(`type`: String, message: String, percent: Option[Double])
    extends JsonSupport {

  def typeName: String = `type`
}
