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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.json.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

class EurekaGroupsLookupSuite extends AnyFunSuite {

  import EurekaSource._
  import Evaluator._

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  private val eurekaGroup = EurekaSource.VipResponse(
    uri = "http://eureka/v2/vips/atlas-lwcapi:7001",
    applications = EurekaSource.Apps(
      List(
        EurekaSource.App("one", mkInstances("one", 5)),
        EurekaSource.App("two", mkInstances("two", 3))
      )
    )
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

  private def lookupFlow: Flow[DataSources, Source[SourcesAndGroups, NotUsed], NotUsed] = {
    val client = Flow[(HttpRequest, AccessLogger)]
      .map {
        case (_, v) =>
          val json = Json.encode(eurekaGroup)
          Success(HttpResponse(StatusCodes.OK, entity = json)) -> v
      }
    val context = TestContext.createContext(mat, client)
    Flow[DataSources].via(new EurekaGroupsLookup(context, 5.microseconds))
  }

  private def run(input: List[DataSources], n: Int = 1): List[SourcesAndGroups] = {
    val future = Source(input)
      .concat(Source.repeat(input.last)) // Need to avoid source stopping until sink is full
      .via(lookupFlow)
      .flatMapConcat(s => s)
      .take(n)
      .fold(List.empty[SourcesAndGroups]) { (acc, v) =>
        v :: acc
      }
      .runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  test("no data sources") {
    val input = List(
      DataSources.empty()
    )
    val future = Source(input)
      .via(lookupFlow)
      .runWith(Sink.seq)
    val output = Await.result(future, Duration.Inf)
    assert(output.isEmpty)
  }

  test("one data source") {
    val input = List(
      sources(ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    )
    val output = run(input)
    assert(output.head._2.groups.size === 1)
    assert(output.head._2.groups.head === eurekaGroup)
  }

  test("unknown data source") {
    val input = List(
      sources(ds("a", "http://unknown/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    )
    val output = run(input)
    assert(output.head._2.groups.size === 0)
    // TODO: check for diagnostic message
  }

  test("groups for data source are refreshed") {
    val input = List(
      sources(ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    )
    val output = run(input, 5)
    assert(output.size === 5)
    output.foreach {
      case (_, g) => assert(g.groups.size === 1)
    }
  }
}
