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
import spray.http.{HttpResponse, StatusCodes}

class RegisterActor extends Actor with ActorLogging {
  import com.netflix.atlas.lwcapi.RegisterApi._

  private val pubsubActor = context.actorSelection("/user/lwc.expressiondb")

  def receive = {
    case RegisterRequest(sinkId, Nil) =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty payload")
    case RegisterRequest(sinkId, expressions) =>
      update(sinkId, expressions)
      sender() ! HttpResponse(StatusCodes.OK)
    case DeleteRequest(sinkId, expressions) =>
      delete(sinkId, expressions)
      sender() ! HttpResponse(StatusCodes.OK)
    case _ =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "unknown payload")
  }

  private def update(sinkId: Option[String], expressions: List[ExpressionWithFrequency]): Unit = {
    expressions.foreach { expr =>
      pubsubActor ! ExpressionDatabaseActor.Publish(expr)
    }
  }

  private def delete(sinkId: Option[String], expressions: List[ExpressionWithFrequency]): Unit = {
    expressions.foreach { expr =>
     pubsubActor ! ExpressionDatabaseActor.Unpublish(expr)
    }
  }
}
