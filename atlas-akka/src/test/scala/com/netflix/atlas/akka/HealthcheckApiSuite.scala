/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.akka

import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.Service
import com.netflix.iep.service.ServiceManager
import org.scalatest.FunSuite
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest


class HealthcheckApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val services = new java.util.HashSet[Service]
  services.add(new AbstractService() {
      override def startImpl(): Unit = ()
      override def stopImpl(): Unit = ()
    })

  val serviceManager = new ServiceManager(services)
  val endpoint = new HealthcheckApi(system, serviceManager)

  test("/healthcheck pre-start") {
    Get("/healthcheck") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.InternalServerError)
    }
  }

  test("/healthcheck post-start") {
    import scala.collection.JavaConversions._
    services.foreach(_.asInstanceOf[AbstractService].start())
    Get("/healthcheck") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }
}

