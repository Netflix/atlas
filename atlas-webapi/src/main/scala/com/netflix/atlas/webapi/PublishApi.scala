/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.webapi

import akka.actor.ActorRefFactory
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteResult
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.validation.Rule
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.config.ConfigManager
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Promise

class PublishApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi with StrictLogging {

  import com.netflix.atlas.webapi.PublishApi._

  private val publishRef = actorRefFactory.actorSelection("/user/publish")

  private val config = ConfigManager.dynamicConfig().getConfig("atlas.webapi.publish")

  private val internWhileParsing = config.getBoolean("intern-while-parsing")

  private val rules = ApiSettings.validationRules

  def routes: Route = {
    post {
      endpointPath("api" / "v1" / "publish") {
        handleReq
      } ~
      endpointPath("api" / "v1" / "publish-fast") {
        // Legacy path from when there was more than one publish mode
        handleReq
      }
    }
  }

  private def handleReq: Route = {
    extractRequestContext { ctx =>
      parseEntity(customJson(p => PublishPayloads.decodeBatch(p, internWhileParsing))) { values =>
        val (good, bad) = validate(values)
        val promise = Promise[RouteResult]()
        val req = PublishRequest(good, bad, promise, ctx)
        publishRef ! req
        _ => promise.future
      }
    }
  }

  private def validate(vs: List[DatapointTuple]): (List[DatapointTuple], List[ValidationResult]) = {
    val validDatapoints = List.newBuilder[DatapointTuple]
    val failures = List.newBuilder[ValidationResult]
    val now = System.currentTimeMillis()
    val limit = ApiSettings.maxDatapointAge
    vs.foreach { v =>
      val diff = now - v.timestamp
      val result = diff match {
        case d if d > limit =>
          val msg = s"data is too old: now = $now, timestamp = ${v.timestamp}, $d > $limit"
          ValidationResult.Fail("DataTooOld", msg, v.tags)
        case d if d < -limit =>
          val msg = s"data is from future: now = $now, timestamp = ${v.timestamp}"
          ValidationResult.Fail("DataFromFuture", msg, v.tags)
        case _ =>
          Rule.validate(v.tags, rules)
      }
      if (result.isSuccess) {
        validDatapoints += v
      } else {
        failures += result
        logger.trace(s"rejected datapoint: $v, reason: $result")
      }
    }
    validDatapoints.result() -> failures.result()
  }
}

object PublishApi {

  type TagMap = Map[String, String]

  case class PublishRequest(
    values: List[DatapointTuple],
    failures: List[ValidationResult],
    promise: Promise[RouteResult],
    ctx: RequestContext
  ) {

    private implicit val ec = ctx.executionContext

    def complete(res: HttpResponse): Unit = {
      ctx.complete(res).onComplete(promise.complete)
    }
  }

  case class FailureMessage(`type`: String, errorCount: Int, message: List[String])
      extends JsonSupport {

    def typeName: String = `type`
  }

  object FailureMessage {

    private def createMessage(level: String, message: List[ValidationResult]): FailureMessage = {
      val failures = message.collect {
        case msg: ValidationResult.Fail => msg
      }
      // Limit encoding the tags to just the summary set
      val summary = failures.take(5).map { msg =>
        s"${msg.reason} (tags=${Json.encode(msg.tags)})"
      }
      new FailureMessage(level, failures.size, summary)
    }

    def error(message: List[ValidationResult]): FailureMessage = {
      createMessage(DiagnosticMessage.Error, message)
    }

    def partial(message: List[ValidationResult]): FailureMessage = {
      createMessage("partial", message)
    }
  }
}
