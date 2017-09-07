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
package com.netflix.atlas.lwcapi

import javax.inject.Inject

import akka.actor.ActorRefFactory
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

class SubscribeApi @Inject()(
  registry: Registry,
  sm: ActorSubscriptionManager,
  splitter: ExpressionSplitter,
  implicit val actorRefFactory: ActorRefFactory
) extends WebApi
    with StrictLogging {

  import SubscribeApi._
  import StreamApi._

  private val evalsId = registry.createId("atlas.lwcapi.subscribe.count")
  private val itemsId = registry.createId("atlas.lwcapi.subscribe.itemCount")

  private val subscribeRef = actorRefFactory.actorSelection("/user/lwc.subscribe")

  def routes: Route = {
    path("lwc" / "api" / "v1" / "subscribe") {
      post {
        parseEntity(json[SubscribeRequest]) {
          case SubscribeRequest(_, Nil) =>
            complete(DiagnosticMessage.error(StatusCodes.BadRequest, "empty payload"))
          case SubscribeRequest(streamId, expressions) =>
            val errors = subscribe(streamId, expressions)
            val errorResponse = if (errors.isEmpty) {
              Errors("success", "success", List())
            } else {
              Errors("error", "Some expressions could not be parsed", errors)
            }
            val entity = HttpEntity(MediaTypes.`application/json`, errorResponse.toJson)
            complete(HttpResponse(StatusCodes.OK, entity = entity))
        }
      }
    }
  }

  private def subscribe(streamId: String, expressions: List[ExpressionMetadata]): List[ErrorMsg] = {
    registry.counter(evalsId.withTag("action", "subscribe")).increment()
    registry.counter(itemsId.withTag("action", "subscribe")).increment(expressions.size)

    val errors = scala.collection.mutable.ListBuffer[ErrorMsg]()
    expressions.foreach { expr =>
      try {
        val splits = splitter.split(expr.expression, expr.frequency)

        // Add any new expressions
        splits.foreach { sub =>
          sm.subscribe(streamId, sub) ! SSESubscribe(expr.expression, List(sub.metadata))
        }

        // Remove any expressions that are no longer required
        val subIds = splits.map(_.metadata.id).toSet
        sm.subscriptionsForStream(streamId)
          .filter(s => !subIds.contains(s.metadata.id))
          .foreach(s => sm.unsubscribe(streamId, s.metadata.id))
      } catch {
        case NonFatal(e) =>
          logger.error(s"Unable to subscribe to expression ${expr.expression}", e)
          errors += ErrorMsg(expr.expression, e.getMessage)
      }
    }
    errors.toList
  }
}

object SubscribeApi {

  case class SubscribeRequest(streamId: String, expressions: List[ExpressionMetadata])
      extends JsonSupport {

    require(streamId != null && !streamId.isEmpty, "streamId attribute is missing or empty")
    require(
      expressions != null && expressions.nonEmpty,
      "expressions attribute is missing or empty"
    )
  }

  case class ErrorMsg(expression: String, message: String)

  case class Errors(`type`: String, message: String, errors: List[ErrorMsg]) extends JsonSupport
}
