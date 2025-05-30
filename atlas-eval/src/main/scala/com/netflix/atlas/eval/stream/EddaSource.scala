/*
 * Copyright 2014-2025 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.ByteStringInputStream
import com.netflix.atlas.pekko.PekkoHttpClient
import com.typesafe.scalalogging.StrictLogging

import scala.util.Failure
import scala.util.Success

private[stream] object EddaSource extends StrictLogging {

  /**
    * Subscribes to all instances that are available for an app or a vip in eureka.
    *
    * @param eddaUri
    *     Should be the `/api/v2/group/autoScalingGroups` endpoint for an Edda service.
    * @param context
    *     Shared context for the evaluation stream.
    */
  def apply(eddaUri: String, context: StreamContext): Source[GroupResponse, NotUsed] = {

    val headers =
      List(Accept(MediaTypes.`application/json`), `Accept-Encoding`(HttpEncodings.gzip))
    val request = HttpRequest(HttpMethods.GET, eddaUri, headers)

    Source
      .single(request)
      .via(context.httpClient("edda"))
      .flatMapConcat {
        case Success(res: HttpResponse) if res.status == StatusCodes.OK =>
          parseResponse(eddaUri, res)
        case Success(res: HttpResponse) =>
          logger.warn(s"edda refresh failed with status ${res.status}: $eddaUri")
          res.discardEntityBytes(context.materializer)
          Source.empty[GroupResponse]
        case Failure(t) =>
          logger.warn(s"edda refresh failed with exception: $eddaUri", t)
          Source.empty[GroupResponse]
      }
  }

  private def parseResponse(
    uri: String,
    res: HttpResponse
  ): Source[GroupResponse, Any] = {
    PekkoHttpClient
      .unzipIfNeeded(res)
      .reduce(_ ++ _)
      .recover {
        case t: Throwable =>
          logger.warn(s"exception while processing edda response: $uri", t)
          ByteString.empty
      }
      .filter(_.nonEmpty)
      .map { bs =>
        decodeEddaResponse(new ByteStringInputStream(bs)).copy(uri = uri)
      }
  }

  private def decodeEddaResponse(in: ByteStringInputStream): EddaResponse = {
    val responses = Json.decode[List[EddaResponse]](in)
    require(responses != null, "EddaResponse list cannot be null")
    EddaResponse(null, responses.flatMap(_.instances))
  }

  //
  // Model objects for Edda response payloads
  //

  case class Groups(groups: List[GroupResponse])

  sealed trait GroupResponse {

    def uri: String
    def instances: List[Instance]
  }

  case class EddaResponse(uri: String, instances: List[Instance]) extends GroupResponse {

    require(instances != null, "instances cannot be null")
  }

  case class Instance(
    instanceId: String,
    privateIpAddress: Option[String] = None,
    ipv6Address: Option[String] = None
  ) {

    require(instanceId != null, "instanceId cannot be null")

    private val variables = {
      val builder = Map.newBuilder[String, String]

      privateIpAddress.foreach { ip =>
        builder.addOne("local-ipv4" -> ip) // Backwards compatibility with Eureka
        builder.addOne("ipv4"       -> ip)
        builder.addOne("ip"         -> ip)
      }

      ipv6Address.foreach { ip =>
        builder.addOne("ipv6" -> s"[$ip]")
        builder.addOne("ip"   -> s"[$ip]")
      }

      // For Edda, always assume default port of 7101
      builder.addOne("port" -> "7101")

      builder.result()
    }

    require(variables.contains("ip"), "IP address must be available")

    def substitute(pattern: String): String = {
      var tmp = pattern
      variables.foreachEntry { (k, v) =>
        tmp = tmp.replace(s"{$k}", v)
      }
      tmp
    }
  }
}
