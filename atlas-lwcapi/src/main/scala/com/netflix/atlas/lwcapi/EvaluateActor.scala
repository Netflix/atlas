/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import akka.actor.{Actor, ActorLogging}
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.model.Datapoint
import spray.http.{HttpResponse, StatusCodes}

class EvaluateActor extends Actor with ActorLogging {

  import com.netflix.atlas.lwcapi.EvaluateApi._

  def receive = {
    case EvaluateRequest(Nil, data) =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty expression payload")
    case EvaluateRequest(expressions, data: List[Datapoint]) =>
      evaluate(expressions, data)
      sender() ! HttpResponse(StatusCodes.OK)
    case _ =>
      sender() ! HttpResponse(StatusCodes.BadRequest, "unknown payload")
  }

  private def evaluate(expressions: List[ExpressionWithFrequency], data: List[Datapoint]): Unit = {
    log.debug("Data: " + data)
    expressions.foreach { expr =>
      log.debug("Expression: " + expr)
    }
  }
}
