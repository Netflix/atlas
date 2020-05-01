/*
 * Copyright 2014-2020 Netflix, Inc.
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
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.Interner
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.Streams
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
      parseEntity(customJson(p => decodeBatch(p, internWhileParsing))) { values =>
        val (good, bad) = validate(values)
        val promise = Promise[RouteResult]()
        val req = PublishRequest(good, bad, promise, ctx)
        publishRef ! req
        _ => promise.future
      }
    }
  }

  private def validate(vs: List[Datapoint]): (List[Datapoint], List[ValidationResult]) = {
    val validDatapoints = List.newBuilder[Datapoint]
    val failures = List.newBuilder[ValidationResult]
    val now = System.currentTimeMillis()
    val limit = ApiSettings.maxDatapointAge
    vs.foreach { v =>
      val diff = now - v.timestamp
      val result = diff match {
        case d if d > limit =>
          val msg = s"data is too old: now = $now, timestamp = ${v.timestamp}, $d > $limit"
          ValidationResult.Fail("DataTooOld", msg)
        case d if d < -limit =>
          val msg = s"data is from future: now = $now, timestamp = ${v.timestamp}"
          ValidationResult.Fail("DataFromFuture", msg)
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

  import com.netflix.atlas.json.JsonParserHelper._

  type TagMap = Map[String, String]

  private final val maxPermittedTags = ApiSettings.maxPermittedTags

  private def decodeTags(parser: JsonParser, commonTags: TagMap, intern: Boolean): TagMap = {
    val strInterner = Interner.forStrings
    val b = new SmallHashMap.Builder[String, String](2 * maxPermittedTags)
    if (commonTags != null) b.addAll(commonTags)
    foreachField(parser) {
      case key =>
        val value = parser.nextTextValue()
        if (value != null) {
          if (intern)
            b.add(strInterner.intern(key), strInterner.intern(value))
          else
            b.add(key, value)
        }
    }
    if (intern) TaggedItem.internTagsShallow(b.compact) else b.result
  }

  private def getValue(parser: JsonParser): Double = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.nextToken() match {
      case START_ARRAY        => nextDouble(parser)
      case VALUE_NUMBER_FLOAT => parser.getValueAsDouble()
      case VALUE_STRING       => java.lang.Double.valueOf(parser.getText())
      case t                  => fail(parser, s"expected VALUE_NUMBER_FLOAT but received $t")
    }
  }

  private def decode(parser: JsonParser, commonTags: TagMap, intern: Boolean): Datapoint = {
    var tags: TagMap = null
    var timestamp: Long = -1L
    var value: Double = Double.NaN
    foreachField(parser) {
      case "tags"      => tags = decodeTags(parser, commonTags, intern)
      case "timestamp" => timestamp = nextLong(parser)
      case "value"     => value = nextDouble(parser)
      case "start"     => timestamp = nextLong(parser) // Legacy support
      case "values"    => value = getValue(parser)
      case _ => // Ignore unknown fields
        parser.nextToken()
        parser.skipChildren()
    }
    Datapoint(tags, timestamp, value)
  }

  def decodeDatapoint(parser: JsonParser, intern: Boolean = false): Datapoint = {
    decode(parser, null, intern)
  }

  def decodeDatapoint(json: String): Datapoint = {
    val parser = Json.newJsonParser(json)
    try decodeDatapoint(parser)
    finally parser.close()
  }

  def decodeBatch(json: String): List[Datapoint] = {
    val parser = Json.newJsonParser(json)
    try decodeBatch(parser)
    finally parser.close()
  }

  def decodeBatch(parser: JsonParser, intern: Boolean = false): List[Datapoint] = {
    var tags: Map[String, String] = null
    var metrics: List[Datapoint] = null
    var tagsLoadedFirst = false
    foreachField(parser) {
      case "tags" => tags = decodeTags(parser, null, intern)
      case "metrics" =>
        tagsLoadedFirst = (tags != null)
        val builder = List.newBuilder[Datapoint]
        foreachItem(parser) { builder += decode(parser, tags, intern) }
        metrics = builder.result
    }

    // If the tags were loaded first they got merged with the datapoints while parsing. Otherwise
    // they need to be merged here.
    if (tagsLoadedFirst || tags == null) {
      if (metrics == null) Nil else metrics
    } else {
      metrics.map(d => d.copy(tags = d.tags ++ tags))
    }
  }

  def decodeList(parser: JsonParser, intern: Boolean = false): List[Datapoint] = {
    val builder = List.newBuilder[Datapoint]
    foreachItem(parser) {
      builder += decode(parser, null, intern)
    }
    builder.result
  }

  def decodeList(json: String): List[Datapoint] = {
    val parser = Json.newJsonParser(json)
    try decodeList(parser)
    finally parser.close()
  }

  private def encodeTags(gen: JsonGenerator, tags: Map[String, String]): Unit = {
    gen.writeObjectFieldStart("tags")
    tags.foreachEntry(gen.writeStringField)
    gen.writeEndObject()
  }

  def encodeDatapoint(gen: JsonGenerator, d: Datapoint): Unit = {
    gen.writeStartObject()
    encodeTags(gen, d.tags)
    gen.writeNumberField("timestamp", d.timestamp)
    gen.writeNumberField("value", d.value)
    gen.writeEndObject()
  }

  def encodeDatapoint(d: Datapoint): String = {
    Streams.string { w =>
      Streams.scope(Json.newJsonGenerator(w)) { gen =>
        encodeDatapoint(gen, d)
      }
    }
  }

  def encodeBatch(gen: JsonGenerator, tags: Map[String, String], values: List[Datapoint]): Unit = {
    gen.writeStartObject()
    encodeTags(gen, tags)
    gen.writeArrayFieldStart("metrics")
    values.foreach(v => encodeDatapoint(gen, v))
    gen.writeEndArray()
    gen.writeEndObject()
  }

  def encodeBatch(tags: Map[String, String], values: List[Datapoint]): String = {
    Streams.string { w =>
      Streams.scope(Json.newJsonGenerator(w)) { gen =>
        encodeBatch(gen, tags, values)
      }
    }
  }

  case class PublishRequest(
    values: List[Datapoint],
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

    def error(message: List[ValidationResult]): FailureMessage = {
      val failures = message.collect { case ValidationResult.Fail(_, reason) => reason }
      new FailureMessage(DiagnosticMessage.Error, failures.size, failures.take(5))
    }

    def partial(message: List[ValidationResult]): FailureMessage = {
      val failures = message.collect { case ValidationResult.Fail(_, reason) => reason }
      new FailureMessage("partial", failures.size, failures.take(5))
    }
  }
}
