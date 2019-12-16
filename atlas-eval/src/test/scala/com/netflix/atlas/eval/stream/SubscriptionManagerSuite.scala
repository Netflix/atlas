/*
 * Copyright 2014-2019 Netflix, Inc.
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

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.AccessLogger
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Success

class SubscriptionManagerSuite extends AnyFunSuite with BeforeAndAfter {

  import EurekaSource._
  import Evaluator._
  import scala.jdk.CollectionConverters._

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  private val group1 = EurekaSource.VipResponse(
    uri = "http://eureka/v2/vips/atlas-lwcapi:7001",
    applications = EurekaSource.Apps(
      List(
        EurekaSource.App("one", mkInstances("one", 5)),
        EurekaSource.App("two", mkInstances("two", 3))
      )
    )
  )

  private val group2 = EurekaSource.AppResponse(
    uri = "http://localhost:7102/v2/vips/local-dev:7001",
    application = EurekaSource.App("three", mkInstances("three", 1))
  )

  private def mkInstances(name: String, n: Int): List[EurekaSource.Instance] = {
    (0 until n).toList.map { i =>
      EurekaSource.Instance(
        instanceId = f"$name-$i%05d",
        status = "UP",
        dataCenterInfo = DataCenterInfo("Amazon", Map("host" -> s"$name.$i")),
        port = PortInfo(7101)
      )
    }
  }

  private def sources(vs: DataSource*): DataSources = {
    DataSources.of(vs: _*)
  }

  private def ds(id: String, uri: String): DataSource = {
    new DataSource(id, java.time.Duration.ofMinutes(1), uri)
  }

  private val requests = new ConcurrentLinkedQueue[HttpRequest]()

  private def run(input: List[SourcesAndGroups]): Unit = {
    val client = Flow[(HttpRequest, AccessLogger)]
      .map {
        case (req, v) =>
          requests.add(req)
          Success(HttpResponse(StatusCodes.OK)) -> v
      }
    val context = TestContext.createContext(mat, client)
    val sink = new SubscriptionManager(context)
    val future = Source(input).runWith(sink)
    Await.result(future, Duration.Inf)
  }

  private def getContent(request: HttpRequest): String = {
    val future = request.entity.dataBytes
      .reduce(_ ++ _)
      .map(_.decodeString(StandardCharsets.UTF_8))
      .runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  before {
    requests.clear()
  }

  test("groups with empty data sources") {
    val input = List(
      DataSources.empty() -> Groups(List(group1, group2))
    )
    run(input)
    assert(requests.isEmpty)
  }

  test("subscribe on group update") {
    val srcs = sources(ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    val input = List(
      srcs -> Groups(List(group1, group2))
    )
    run(input)
    assert(requests.size === 8)
    assert(getContent(requests.peek()).contains("name,jvm.gc.pause,:eq,:dist-avg"))
  }

  test("subscribe on each group update") {
    val srcs = sources(ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    val input = List(
      srcs -> Groups(List(group1, group2)),
      srcs -> Groups(List(group1, group2)),
      srcs -> Groups(List(group1, group2)),
      srcs -> Groups(List(group1, group2))
    )
    run(input)
    assert(requests.size === 8 * 4)
  }

  test("change to no sources") {
    val srcs = sources(ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    val input = List(
      srcs      -> Groups(List(group1, group2)),
      sources() -> Groups(List(group1, group2))
    )
    run(input)
    assert(requests.size === 8)
  }

  test("sources for different vips") {
    val srcs = sources(
      ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"),
      ds("b", "http://localhost:7101/api/v1/graph?q=name,jvm.gc.allocationRate,:eq,:sum")
    )
    val input = List(
      srcs -> Groups(List(group1, group2))
    )
    run(input)
    assert(requests.size === 9)
    requests.asScala.foreach { request =>
      assert(request.uri.authority.port === 7101)
      request.uri.authority.host.toString match {
        case s if s.startsWith("one.") || s.startsWith("two.") =>
          assert(getContent(request).contains("jvm.gc.pause"))
          assert(!getContent(request).contains("jvm.gc.allocationRate"))
        case s if s.startsWith("three.") =>
          assert(!getContent(request).contains("jvm.gc.pause"))
          assert(getContent(request).contains("jvm.gc.allocationRate"))
      }
    }
  }
}
