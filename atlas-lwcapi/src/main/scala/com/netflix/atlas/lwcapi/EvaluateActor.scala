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

import javax.inject.Inject

import akka.actor.{Actor, ActorLogging}
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.lwcapi.StreamApi._
import spray.http.{HttpResponse, StatusCodes}

class EvaluateActor @Inject() (sm: SubscriptionManager) extends Actor with ActorLogging {
  import com.netflix.atlas.lwcapi.EvaluateApi._

  def receive = {
    case EvaluateRequest(Nil) =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty expression payload")
    case EvaluateRequest(items) =>
      evaluate(items)
      sender() ! HttpResponse(StatusCodes.OK)
    case _ =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "unknown payload")
  }

  private def evaluate(items: List[Item]): Unit = {
    log.info("Received an evaluate request")
    items.foreach { item =>
      log.info("Item: " + item)
      val actors = sm.actorsForExpression(item.id)
      val message = SSEEvaluate(item)
      actors.foreach(actor => actor ! message)
    }
  }
}
