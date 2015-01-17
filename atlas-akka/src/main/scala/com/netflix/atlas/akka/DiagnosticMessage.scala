/*
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.atlas.akka

import akka.actor.ActorRef
import com.fasterxml.jackson.core.JsonParseException
import com.netflix.atlas.json.JsonSupport
import spray.http.HttpEntity
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCode
import spray.http.StatusCodes
import spray.routing.RequestContext

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

  def handleException(ref: ActorRef): PartialFunction[Throwable, Unit] = {
    case e @ (_: IllegalArgumentException | _: IllegalStateException | _: JsonParseException) =>
      sendError(ref, StatusCodes.BadRequest, e)
    case e: NoSuchElementException =>
      sendError(ref, StatusCodes.NotFound, e)
    case e: Throwable =>
      sendError(ref, StatusCodes.InternalServerError, e)
  }

  def sendError(ref: ActorRef, status: StatusCode, t: Throwable): Unit = {
    sendError(ref, status, s"${t.getClass.getSimpleName}: ${t.getMessage}")
  }

  def sendError(ref: ActorRef, status: StatusCode, msg: String): Unit = {
    val errorMsg = DiagnosticMessage.error(msg)
    val entity = HttpEntity(MediaTypes.`application/json`, errorMsg.toJson)
    ref ! HttpResponse(status = status, entity = entity)
  }
}

case class DiagnosticMessage(`type`: String, message: String, percent: Option[Double])
    extends JsonSupport {
  def typeName: String = `type`
}
