/*
 * Copyright 2014-2022 Netflix, Inc.
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
import akka.http.scaladsl.model.RemoteAddress
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcDiagnosticMessage
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

class EvaluateApi(registry: Registry, sm: StreamSubscriptionManager)
    extends WebApi
    with StrictLogging {
  import EvaluateApi._

  private val payloadSize = registry.distributionSummary("atlas.lwcapi.evalPayloadSize")
  private val ignoredCounter = registry.counter("atlas.lwcapi.ignoredItems")

  def routes: Route = {
    endpointPath("lwc" / "api" / "v1" / "evaluate") {
      post {
        extractClientIP { addr =>
          parseEntity(json[EvaluateRequest]) { req =>
            payloadSize.record(req.metrics.size)
            val timestamp = req.timestamp
            req.metrics.groupBy(_.id).foreach {
              case (id, ms) =>
                val datapoints = ms.map { m =>
                  LwcDatapoint(timestamp, m.id, m.tags, m.value)
                }
                evaluate(addr, id, datapoints)
            }
            req.messages.groupBy(_.id).foreach {
              case (id, ms) => evaluate(addr, id, ms)
            }
            complete(HttpResponse(StatusCodes.OK))
          }
        }
      }
    }
  }

  private def evaluate(addr: RemoteAddress, id: String, msgs: Seq[JsonSupport]): Unit = {
    val queues = sm.handlersForSubscription(id)
    if (queues.nonEmpty) {
      queues.foreach { queue =>
        logger.trace(s"sending ${msgs.size} messages to $queue (from: $addr)")
        queue.offer(msgs)
      }
    } else {
      logger.debug(s"no subscriptions, ignoring ${msgs.size} messages (from: $addr)")
      ignoredCounter.increment(msgs.size)
    }
  }
}

object EvaluateApi {
  type TagMap = Map[String, String]

  case class Item(id: String, tags: TagMap, value: Double)

  case class EvaluateRequest(
    timestamp: Long,
    metrics: List[Item] = Nil,
    messages: List[LwcDiagnosticMessage] = Nil
  ) extends JsonSupport
}
