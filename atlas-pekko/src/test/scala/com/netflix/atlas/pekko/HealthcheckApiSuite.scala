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
package com.netflix.atlas.pekko

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.iep.service.Service
import com.netflix.iep.service.ServiceManager
import com.netflix.iep.service.State

import java.util.function.Supplier

class HealthcheckApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration.*

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  private val serviceHealth = new AtomicBoolean(false)

  private val services = new java.util.HashSet[Service]

  services.add(new Service {

    override def state(): State = State.RUNNING

    override def name(): String = "test"

    override def isHealthy: Boolean = serviceHealth.get()
  })

  private val serviceManager = new ServiceManager(services)

  private val supplier = new Supplier[ServiceManager] {

    override def get(): ServiceManager = serviceManager
  }
  private val endpoint = new HealthcheckApi(supplier)

  test("/healthcheck pre-start") {
    serviceHealth.set(false)
    Get("/healthcheck") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.InternalServerError)
      val data = Json.decode[Map[String, Boolean]](responseAs[String])
      assert(!data("test"))
    }
  }

  test("/healthcheck post-start") {
    serviceHealth.set(true)
    Get("/healthcheck") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      val data = Json.decode[Map[String, Boolean]](responseAs[String])
      assert(data("test"))
    }
  }
}
