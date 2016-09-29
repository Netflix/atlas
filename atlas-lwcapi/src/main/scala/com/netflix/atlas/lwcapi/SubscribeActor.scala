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

import akka.actor.Actor
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.lwcapi.StreamApi.SSESubscribe
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import spray.http.{HttpResponse, StatusCodes}

import scala.collection.mutable
import scala.util.control.NonFatal

class SubscribeActor @Inject()(sm: SubscriptionManager,
                               splitter: ExpressionSplitter,
                               registry: Registry)
  extends Actor with StrictLogging {

  import com.netflix.atlas.lwcapi.SubscribeApi._

  case class ErrorMsg(expression: String, message: String)
  case class Errors(`type`: String, message: String, errors: List[ErrorMsg]) extends JsonSupport

  private val evalsId = registry.createId("atlas.lwcapi.subscribe.count")
  private val itemsId = registry.createId("atlas.lwcapi.subscribe.itemCount")
  private val uninterestingId = registry.createId("atlas.lwcapi.subscribe.uninterestingCount")

  private val dbActor = context.actorSelection("/user/lwc.expressiondb")

  def receive = {
    case SubscribeRequest(sinkId, Nil) =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty payload")
    case SubscribeRequest(sinkId, expressions) =>
      val errors = subscribe(sinkId, expressions)
      val errorResponse = if (errors.isEmpty) {
        Errors("success", "success", List())
      } else {
        Errors("error", "Some expressions could not be parsed", errors)
      }
      sender() ! HttpResponse(StatusCodes.OK, errorResponse.toJson)
    case _ =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "unknown payload")
  }

  private def subscribe(streamId: String, expressions: List[ExpressionWithFrequency]): List[ErrorMsg] = {
    registry.counter(evalsId.withTag("action", "subscribe")).increment()
    registry.counter(itemsId.withTag("action", "subscribe")).increment(expressions.size)

    val errors = mutable.ListBuffer[ErrorMsg]()
    expressions.foreach { expr =>
      try {
        val split = splitter.split(expr.expression, expr.frequency)
        dbActor ! ExpressionDatabaseActor.Expression(split)
        val registration = sm.registration(streamId)
        if (registration.isDefined) {
          split.expressions.foreach(e =>
            dbActor ! ExpressionDatabaseActor.Subscribe(streamId, e.id)
          )
          registration.get.actorRef ! SSESubscribe(expr.expression, split.expressions)
        } else {
          registry.counter(uninterestingId.withTag("action", "subscribe")).increment()
        }
      } catch {
        case NonFatal(e) =>
          logger.error(s"Unable to subscribe to expression ${expr.expression}", e)
          errors += ErrorMsg(expr.expression, e.getMessage)
      }
    }
    errors.toList
  }
}
