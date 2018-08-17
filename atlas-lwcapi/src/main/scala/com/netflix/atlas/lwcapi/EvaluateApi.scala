/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.lwcapi.StreamApi._
import com.netflix.spectator.api.Registry

class EvaluateApi(registry: Registry, sm: StreamSubscriptionManager) extends WebApi {
  import EvaluateApi._

  private val evalsCounter = registry.counter("atlas.lwcapi.evaluate.count")
  private val itemsCounter = registry.counter("atlas.lwcapi.evaluate.itemCount")
  private val actorsCounter = registry.counter("atlas.lwcapi.evaluate.actorCount")
  private val ignoredCounter = registry.counter("atlas.lwcapi.evaluate.ignoredCount")

  def routes: Route = {
    path("lwc" / "api" / "v1" / "evaluate") {
      post {
        parseEntity(json[EvaluateRequest]) { req =>
          if (req.metrics.isEmpty) {
            complete(DiagnosticMessage.error(StatusCodes.BadRequest, "empty metrics list"))
          } else {
            evaluate(req.timestamp, req.metrics)
            complete(HttpResponse(StatusCodes.OK))
          }
        }
      }
    }
  }

  private def evaluate(timestamp: Long, items: List[Item]): Unit = {
    evalsCounter.increment()
    itemsCounter.increment(items.size)
    items.foreach { item =>
      val actors = sm.handlersForSubscription(item.id)
      if (actors.nonEmpty) {
        actorsCounter.increment(actors.size)
        val message = SSEMetric(timestamp, item)
        actors.foreach(_.offer(message))
      } else {
        ignoredCounter.increment()
      }
    }
  }
}

object EvaluateApi {
  type TagMap = Map[String, String]

  case class Item(id: String, tags: TagMap, value: Double) extends JsonSupport

  case class EvaluateRequest(timestamp: Long, metrics: List[Item]) extends JsonSupport
}
