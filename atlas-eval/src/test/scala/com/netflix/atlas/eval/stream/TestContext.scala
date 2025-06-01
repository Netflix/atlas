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

import com.netflix.atlas.pekko.PekkoHttpClient
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.stream.Materializer
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.ConfigFactory
import org.apache.pekko.http.scaladsl.model.StatusCodes

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
      |      edda-uri = "http://localhost:7102/v2/vips/local-dev:7001"
      |      instance-uri = "http://{host}:{port}"
      |    },
      |    {
      |      host = "atlas"
      |      edda-uri = "http://edda/v2/autoScalingGroups/atlas-lwcapi:7001"
      |      instance-uri = "http://{host}:{port}"
      |    }
      |  ]
      |
      |  num-buffers = 2
      |
      |  limits {
      |    max-input-datapoints = 50000
      |    max-intermediate-datapoints = 10000000
      |    max-step = 60s
      |  }
      |
      |  expression-limit = 50000
      |
      |  ignored-tag-keys = []
      |}
      |
      |atlas.eval.graph {
      |  step = 60s
      |  block-size = 60
      |  start-time = e-3h
      |  end-time = now
      |  timezone = US/Pacific
      |  width = 700
      |  height = 300
      |  theme = "light"
      |
      |  light {
      |    palette {
      |      primary = "armytage"
      |      offset = "bw"
      |    }
      |    named-colors = {
      |    }
      |  }
      |
      |  max-datapoints = 1440
      |  png-metadata-enabled = false
      |  browser-agent-pattern = "mozilla|msie|gecko|chrome|opera|webkit"
      |  simple-legends-enabled = false
      |  engines = []
      |  vocabulary = "default"
      |}
      |
      |atlas.eval.host-rewrite {
      |  pattern = "$^"
      |  key = ""
      |}
    """.stripMargin)

  def createContext(
    mat: Materializer,
    response: Try[HttpResponse] = Success(HttpResponse(StatusCodes.OK)),
    registry: Registry = new NoopRegistry
  ): StreamContext = {
    new StreamContext(config, mat, registry) {

      override def httpClient(name: String): SimpleClient = {
        PekkoHttpClient.create(response).simpleFlow()
      }
    }
  }
}
