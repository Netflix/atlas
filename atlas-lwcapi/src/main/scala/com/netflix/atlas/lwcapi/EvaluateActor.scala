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
import com.netflix.spectator.api.Registry
import spray.http.{HttpResponse, StatusCodes}

class EvaluateActor @Inject() (registry: Registry, sm: SubscriptionManager) extends Actor with ActorLogging {
  import com.netflix.atlas.lwcapi.EvaluateApi._

  private val evalsId = registry.createId("atlas.lwcapi.evaluate.count")
  private val itemsId = registry.createId("atlas.lwcapi.evaluate.itemCount")
  private val actorsId = registry.createId("atlas.lwcapi.evaluate.actorCount")
  private val uninterestingId = registry.createId("atlas.lwcapi.evaluate.uninterestingCount")

  def receive = {
    case EvaluateRequest(_, Nil) =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty expression payload")
    case EvaluateRequest(timestamp, items) =>
      evaluate(timestamp, items)
      sender() ! HttpResponse(StatusCodes.OK)
    case _ =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "unknown payload")
  }

  private def evaluate(timestamp: Long, items: List[Item]): Unit = {
    registry.counter(evalsId).increment()
    registry.counter(itemsId).increment(items.size)
    items.foreach { item =>
      val actors = sm.actorsForExpression(item.id)
      if (actors.nonEmpty) {
        registry.counter(actorsId).increment(actors.size)
        val message = SSEMetric(timestamp, item)
        actors.foreach(actor => actor ! message)
      } else {
        registry.counter(uninterestingId).increment()
      }
    }
  }
}
