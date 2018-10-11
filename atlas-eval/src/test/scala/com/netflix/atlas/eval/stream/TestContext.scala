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
package com.netflix.atlas.eval.stream

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import com.netflix.atlas.akka.AccessLogger
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory

import scala.util.Success
import scala.util.Try

object TestContext {

  private val config =
    ConfigFactory.parseString("""
      |atlas.core.vocabulary {
      |  words = []
      |  custom-averages = []
      |}
      |
      |atlas.eval.stream {
      |  backends = [
      |    {
      |      host = "localhost"
      |      eureka-uri = "http://localhost:7102/v2/vips/local-dev:7001"
      |      instance-uri = "http://{host}:{port}"
      |    },
      |    {
      |      host = "atlas"
      |      eureka-uri = "http://eureka/v2/vips/atlas-lwcapi:7001"
      |      instance-uri = "http://{host}:{port}"
      |    }
      |  ]
      |
      |  num-buffers = 2
      |
      |  ignored-tag-keys = []
      |}
    """.stripMargin)

  def createContext(
    mat: ActorMaterializer,
    client: Client = defaultClient,
    registry: Registry = new NoopRegistry
  ): StreamContext = {
    new StreamContext(config, client, mat, registry)
  }

  def defaultClient: Client = client(HttpResponse(StatusCodes.OK))

  def client(response: HttpResponse): Client = client(Success(response))

  def client(result: Try[HttpResponse]): Client = {
    Flow[(HttpRequest, AccessLogger)].map { case (_, v) => result -> v }
  }
}
