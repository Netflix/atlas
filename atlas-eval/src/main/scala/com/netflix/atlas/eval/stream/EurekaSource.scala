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
package com.netflix.atlas.eval.stream

import akka.NotUsed
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.util.Failure
import scala.util.Success

private[stream] object EurekaSource extends StrictLogging {

  type ResponseFlow = Flow[NotUsed, GroupResponse, NotUsed]

  /**
    * Subscribes to all instances that are available for an app or a vip in eureka.
    *
    * @param eurekaUri
    *     Should be either the `/v2/apps/{app}` or `/v2/vips/{vip}` endpoint for a
    *     Eureka service. Depending on the Eureka service configuration there may be
    *     some variation in the path.
    * @param client
    *     HTTP client flow used for getting data from Eureka and for consuming from
    *     the instances.
    */
  def apply(eurekaUri: String, client: Client): Source[GroupResponse, NotUsed] = {
    Source.single(NotUsed).via(fetchEurekaData(eurekaUri, client))
  }

  private def fetchEurekaData(eurekaUri: String, client: Client): ResponseFlow = {
    val useVipFormat = eurekaUri.contains("/vips/")
    val headers =
      List(Accept(MediaTypes.`application/json`), `Accept-Encoding`(HttpEncodings.gzip))
    val request = HttpRequest(HttpMethods.GET, eurekaUri, headers)

    Flow[NotUsed]
      .map(_ => request -> NotUsed)
      .via(client)
      .flatMapConcat {
        case (Success(res: HttpResponse), _) if res.status == StatusCodes.OK =>
          parseResponse(eurekaUri, res, useVipFormat)
        case (Success(res: HttpResponse), _) =>
          logger.warn(s"eureka refresh failed with status ${res.status}: $eurekaUri")
          Source.empty[GroupResponse]
        case (Failure(t), _) =>
          logger.warn(s"eureka refresh failed with exception: $eurekaUri", t)
          Source.empty[GroupResponse]
      }
  }

  private def unzipIfNeeded(res: HttpResponse): Source[ByteString, Any] = {
    val isCompressed = res.headers.contains(`Content-Encoding`(HttpEncodings.gzip))
    if (isCompressed) res.entity.dataBytes.via(Compression.gunzip()) else res.entity.dataBytes
  }

  private def parseResponse(
    uri: String,
    res: HttpResponse,
    vipFormat: Boolean
  ): Source[GroupResponse, Any] = {
    unzipIfNeeded(res)
      .reduce(_ ++ _)
      .recover {
        case t: Throwable =>
          logger.warn(s"exception while processing eureka response: $uri", t)
          ByteString.empty
      }
      .filter(_.nonEmpty)
      .map { bs =>
        if (vipFormat)
          Json.decode[VipResponse](bs.toArray).copy(uri = uri)
        else
          Json.decode[AppResponse](bs.toArray).copy(uri = uri)
      }
  }

  //
  // Model objects for Eureka response payloads
  //

  case class Groups(groups: List[GroupResponse])

  sealed trait GroupResponse {
    def uri: String
    def instances: List[Instance]
  }

  case class VipResponse(uri: String, applications: Apps) extends GroupResponse {
    require(applications != null, "applications cannot be null")
    def instances: List[Instance] = applications.application.flatMap(_.instance)
  }

  case class AppResponse(uri: String, application: App) extends GroupResponse {
    require(application != null, "application cannot be null")
    def instances: List[Instance] = application.instance
  }

  case class Apps(application: List[App]) {
    require(application != null, "application cannot be null")
  }

  case class App(name: String, instance: List[Instance]) {
    require(instance != null, "instance cannot be null")
  }

  case class Instance(
    instanceId: String,
    status: String,
    dataCenterInfo: DataCenterInfo,
    port: PortInfo
  ) {
    require(instanceId != null, "instanceId cannot be null")
    require(status != null, "status cannot be null")
    require(dataCenterInfo != null, "dataCenterInfo cannot be null")
    require(port != null, "port cannot be null")

    def substitute(pattern: String): String = {
      var tmp = pattern
      dataCenterInfo.metadata.foreach {
        case (k, v) =>
          tmp = tmp.replace(s"{$k}", v)
      }
      tmp = tmp.replace("{port}", port.toString)
      tmp
    }
  }

  case class DataCenterInfo(name: String, metadata: Map[String, String]) {
    require(metadata != null, "metadata cannot be null")
  }

  case class PortInfo(`$`: Int = 7101) {

    def port: Int = `$`

    override def toString: String = `$`.toString
  }
}
