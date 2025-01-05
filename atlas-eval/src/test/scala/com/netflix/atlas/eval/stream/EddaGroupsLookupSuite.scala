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
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.json.Json
import munit.FunSuite
import org.apache.pekko.stream.Materializer

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Success

class EddaGroupsLookupSuite extends FunSuite {

  import Evaluator.*

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val mat: Materializer = Materializer(system)

  private val eddaGroup = EddaSource.EddaResponse(
    uri = "http://edda/v2/autoScalingGroups/atlas-lwcapi:7001",
    instances = mkInstances("one", 5) ::: mkInstances("two", 5)
  )

  private def mkInstances(name: String, n: Int): List[EddaSource.Instance] = {
    (0 until n).toList.map { i =>
      EddaSource.Instance(
        instanceId = f"$name-$i%05d",
        privateIpAddress = Some(s"$name.$i")
      )
    }
  }

  private def sources(vs: DataSource*): DataSources = {
    DataSources.of(vs*)
  }

  private def ds(id: String, uri: String): DataSource = {
    new DataSource(id, java.time.Duration.ofMinutes(1), uri)
  }

  private def lookupFlow: Flow[DataSources, Source[SourcesAndGroups, NotUsed], NotUsed] = {
    val json = Json.encode(List(eddaGroup))
    val response = Success(HttpResponse(StatusCodes.OK, entity = json))
    val context = TestContext.createContext(mat, response)
    Flow[DataSources].via(new EddaGroupsLookup(context, 5.microseconds))
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

  test("empty sources produces 1 empty sources") {
    val input = List(
      DataSources.empty()
    )
    val output = run(input)
    assertEquals(output.size, 1)
    assertEquals(output.head._1.sources().size(), 0)
    assertEquals(output.head._2.groups.size, 0)
  }

  test("one data source") {
    val input = List(
      sources(ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    )
    val output = run(input)
    assertEquals(output.head._2.groups.size, 1)
    assertEquals(output.head._2.groups.head, eddaGroup)
  }

  test("unknown data source") {
    val input = List(
      sources(ds("a", "http://unknown/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    )
    val output = run(input)
    assertEquals(output.head._2.groups.size, 0)
    // TODO: check for diagnostic message
  }

  test("groups for data source are refreshed") {
    val input = List(
      sources(ds("a", "http://atlas/api/v1/graph?q=name,jvm.gc.pause,:eq,:dist-avg"))
    )
    val output = run(input, 5)
    assertEquals(output.size, 5)
    output.foreach {
      case (_, g) => assertEquals(g.groups.size, 1)
    }
  }
}
