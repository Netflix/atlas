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

import javax.inject.Inject

import akka.actor.ActorRefFactory
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

case class ExpressionApi @Inject()(
  sm: StreamSubscriptionManager,
  registry: Registry,
  implicit val actorRefFactory: ActorRefFactory
) extends WebApi
    with StrictLogging {

  import ExpressionApi._

  private val expressionFetchesId = registry.createId("atlas.lwcapi.expressions.fetches")
  private val expressionCount = registry.distributionSummary("atlas.lwcapi.expressions.count")

  def routes: Route = {
    endpointPathPrefix("lwc" / "api" / "v1" / "expressions") {
      optionalHeaderValueByName("If-None-Match") { etags =>
        get {
          pathEndOrSingleSlash {
            complete(handleList(etags))
          } ~
          path(Segment) { (cluster) =>
            complete(handleGet(etags, cluster))
          }
        }
      }
    }
  }

  private def handleList(receivedETags: Option[String]): HttpResponse = {
    handle(receivedETags, sm.subscriptions.map(_.metadata))
  }

  private def handleGet(receivedETags: Option[String], cluster: String): HttpResponse = {
    handle(receivedETags, sm.subscriptionsForCluster(cluster).map(_.metadata))
  }

  private def handle(
    receivedETags: Option[String],
    expressions: List[ExpressionMetadata]
  ): HttpResponse = {
    expressionCount.record(expressions.size)
    val tag = computeETag(expressions)
    val headers: List[HttpHeader] = List(RawHeader("ETag", tag))
    val recvTags = receivedETags.getOrElse("")
    if (recvTags.contains(tag)) {
      registry.counter(expressionFetchesId.withTag("etagmatch", "true")).increment()
      HttpResponse(StatusCodes.NotModified, headers = headers)
    } else {
      registry.counter(expressionFetchesId.withTag("etagmatch", "false")).increment()
      val json = Return(expressions).toJson
      HttpResponse(StatusCodes.OK, headers, HttpEntity(MediaTypes.`application/json`, json))
    }
  }
}

object ExpressionApi {

  case class Return(expressions: List[ExpressionMetadata]) extends JsonSupport

  private[lwcapi] def computeETag(expressions: List[ExpressionMetadata]): String = {

    // TODO: This should get refactored so we do not need to recompute each time
    val str = expressions.sorted.mkString(";")
    val hash = Strings.zeroPad(Hash.sha1bytes(str), 40).substring(20)
    '"' + hash + '"'
  }
}
