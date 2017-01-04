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

import java.security.MessageDigest
import java.util.Base64
import javax.inject.Inject

import akka.actor.ActorRefFactory
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.json.{Json, JsonSupport}
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import spray.http.HttpHeaders.RawHeader
import spray.http.{HttpHeader, HttpResponse, StatusCodes}
import spray.routing.RequestContext

case class ExpressionApi @Inject()(expressionDatabase: ExpressionDatabase,
  registry: Registry,
  implicit val actorRefFactory: ActorRefFactory)
  extends WebApi with StrictLogging {
  import ExpressionApi._

  private val expressionFetchesId = registry.createId("atlas.lwcapi.expressions.fetches")
  private val expressionCount = registry.distributionSummary("atlas.lwcapi.expressions.count")

  def routes: RequestContext => Unit = {
    path("lwc" / "api" / "v1" / "expressions" / Segment) { (cluster) =>
      optionalHeaderValueByName("If-None-Match") { etags =>
        get { ctx => handleReq(ctx, etags, cluster) }
      }
    }
  }

  private def handleReq(ctx: RequestContext, received_etags: Option[String], cluster: String): Unit = {
    val expressions = expressionDatabase.expressionsForCluster(cluster)
    val tag = compute_etag(expressions)
    val headers: List[HttpHeader] = List(RawHeader("ETag", tag))
    val sent_tags = split_sent_tags(received_etags)
    if (sent_tags.contains(tag)) {
      ctx.responder ! HttpResponse(StatusCodes.NotModified, headers = headers)
      registry.counter(expressionFetchesId.withTag("etagmatch", "true")).increment()
    } else {
      val json = Return(expressions).toJson
      ctx.responder ! HttpResponse(StatusCodes.OK, entity = json, headers = headers)
      registry.counter(expressionFetchesId.withTag("etagmatch", "false")).increment()
    }
    expressionCount.record(expressions.size)
  }
}

object ExpressionApi {
  case class Return(expressions: List[ExpressionWithFrequency]) extends JsonSupport

  private[lwcapi] def compute_etag(expressions: List[ExpressionWithFrequency]): String = {
    val md = MessageDigest.getInstance("SHA-1")
    md.reset()
    expressions.sorted.foreach { e => { md.update(e.toString.getBytes("UTF-8"))}}
    '"' + Base64.getUrlEncoder.withoutPadding.encodeToString(md.digest()) + '"'
  }

  private[lwcapi] def split_sent_tags(tags: Option[String]): List[String] = {
    if (tags.isDefined)
      tags.get.split(",").map {t => t.trim}.toList
    else
      List()
  }
}
