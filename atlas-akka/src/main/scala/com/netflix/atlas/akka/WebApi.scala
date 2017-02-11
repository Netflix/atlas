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
package com.netflix.atlas.akka

import com.fasterxml.jackson.core.JsonParser
import com.netflix.atlas.json.Json
import spray.http._
import spray.routing._


/**
 * Base trait for classes providing an API to expose via the Atlas server.
 */
trait WebApi extends HttpService {

  def routes: RequestContext => Unit

  protected def getJsonParser(request: HttpRequest): Option[JsonParser] = {
    request.entity match {
      case entity: HttpEntity.NonEmpty =>
        if (entity.contentType.mediaType.subType == "x-jackson-smile")
          Some(Json.newSmileParser(entity.data.toByteArray))
        else
          Some(Json.newJsonParser(entity.data.asString(HttpCharsets.`UTF-8`)))
      case _ => None
    }
  }

  protected def handleException(ctx: RequestContext): PartialFunction[Throwable, Unit] = {
    DiagnosticMessage.handleException(ctx.responder)
  }

  protected def sendError(ctx: RequestContext, status: StatusCode, t: Throwable): Unit = {
    DiagnosticMessage.sendError(ctx.responder, status, t)
  }

  protected def sendError(ctx: RequestContext, status: StatusCode, msg: String): Unit = {
    DiagnosticMessage.sendError(ctx.responder, status, msg)
  }
}
