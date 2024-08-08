/*
 * Copyright 2014-2024 Netflix, Inc.
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

import com.fasterxml.jackson.databind.JsonMappingException
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.PekkoHttpClient
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import munit.FunSuite
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.testkit.TestKitBase

import java.time.Duration
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class DataSourceRewriterSuite extends FunSuite with TestKitBase {

  val dss = DataSources.of(
    new DataSource("foo", Duration.ofSeconds(60), "http://localhost/api/v1/graph?q=name,foo,:eq"),
    new DataSource(
      "bar",
      Duration.ofSeconds(60),
      "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and"
    )
  )

  var config: Config = _
  var logger: MockLogger = _
  var ctx: StreamContext = null

  override implicit def system: ActorSystem = ActorSystem("Test")

  override def beforeEach(context: BeforeEach): Unit = {
    config = ConfigFactory
      .load()
      .withValue(
        "atlas.eval.stream.rewrite-url",
        ConfigValueFactory.fromAnyRef("http://localhost/api/v1/rewrite")
      )
    logger = new MockLogger()
    ctx = new StreamContext(config, Materializer(system), dsLogger = logger)
  }

  test("rewrite: Disabled") {
    config = ConfigFactory.load()
    val obtained = rewrite(dss, null)
    assertEquals(obtained, dss)
  }

  test("rewrite: OK") {
    val client = mockClient(
      StatusCode.int2StatusCode(200),
      Map(
        "http://localhost/api/v1/graph?q=name,foo,:eq" -> Rewrite(
          "OK",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          ""
        ),
        "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and" -> Rewrite(
          "OK",
          "http://atlas-seg.prod.netflix.net/api/v1/graph?q=name,bar,:eq",
          "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and",
          ""
        )
      )
    )
    val expected = DataSources.of(
      new DataSource("foo", Duration.ofSeconds(60), "http://localhost/api/v1/graph?q=name,foo,:eq"),
      new DataSource(
        "bar",
        Duration.ofSeconds(60),
        "http://atlas-seg.prod.netflix.net/api/v1/graph?q=name,bar,:eq"
      )
    )
    val obtained = rewrite(dss, client)
    assertEquals(obtained, expected)
    ctx.dsLogger.asInstanceOf[MockLogger].assertSize(0)
  }

  test("parseRewrites: Bad URI in datasources") {
    val client = mockClient(
      StatusCode.int2StatusCode(200),
      Map(
        "http://localhost/api/v1/graph?q=name,foo,:eq" -> Rewrite(
          "OK",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          ""
        ),
        "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and" -> Rewrite(
          "NOT_FOUND",
          "http://atlas-seg.prod.netflix.net/api/v1/graph?q=name,bar,:eq",
          "http://atlas-seg.prod.netflix.net/api/v1/graph?q=name,bar,:eq",
          "No namespace found for seg"
        )
      )
    )
    val expected = DataSources.of(
      new DataSource("foo", Duration.ofSeconds(60), "http://localhost/api/v1/graph?q=name,foo,:eq")
    )
    val obtained = rewrite(dss, client)
    assertEquals(obtained, expected)
    ctx.dsLogger.asInstanceOf[MockLogger].assertSize(1)
  }

  test("parseRewrites: Malformed response JSON") {
    val client = mockClient(
      StatusCode.int2StatusCode(200),
      Map(
        "http://localhost/api/v1/graph?q=name,foo,:eq" -> Rewrite(
          "OK",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          ""
        ),
        "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and" -> Rewrite(
          "OK",
          "http://atlas-seg.prod.netflix.net/api/v1/graph?q=name,bar,:eq",
          "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and",
          ""
        )
      ),
      malformed = true
    )
    intercept[JsonMappingException] {
      rewrite(dss, client)
    }
  }

  test("parseRewrites: 500") {
    val client = mockClient(
      StatusCode.int2StatusCode(500),
      Map.empty
    )
    intercept[RuntimeException] {
      rewrite(dss, client)
    }
  }

  test("parseRewrites: Missing a rewrite") {
    val client = mockClient(
      StatusCode.int2StatusCode(200),
      Map(
        "http://localhost/api/v1/graph?q=name,foo,:eq" -> Rewrite(
          "OK",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          "http://localhost/api/v1/graph?q=name,foo,:eq",
          ""
        )
      )
    )
    val expected = DataSources.of(
      new DataSource("foo", Duration.ofSeconds(60), "http://localhost/api/v1/graph?q=name,foo,:eq")
    )
    val obtained = rewrite(dss, client)
    assertEquals(obtained, expected)
    ctx.dsLogger.asInstanceOf[MockLogger].assertSize(1)
  }

  def mockClient(
    status: StatusCode,
    response: Map[String, Rewrite],
    returnEx: Option[Exception] = None,
    malformed: Boolean = false
  ): SuperPoolClient = {
    returnEx
      .map { ex =>
        PekkoHttpClient.create(Failure(ex)).superPool[List[DataSource]]()
      }
      .getOrElse {
        new MockClient(status, response, malformed).superPool[List[DataSource]]()
      }
  }

  def rewrite(dss: DataSources, client: SuperPoolClient): DataSources = {
    val rewriter = new DataSourceRewriter(config, system)
    val future = Source
      .single(dss)
      .via(
        rewriter.rewrite(client, ctx)
      )
      .toMat(Sink.head)(Keep.right)
      .run()
    Await.result(future, 30.seconds)
  }

  class MockClient(status: StatusCode, response: Map[String, Rewrite], malformed: Boolean = false)
      extends PekkoHttpClient {

    override def singleRequest(request: HttpRequest): Future[HttpResponse] = ???

    override def superPool[C](
      config: PekkoHttpClient.ClientConfig
    ): Flow[(HttpRequest, C), (Try[HttpResponse], C), NotUsed] = {
      Flow[(HttpRequest, C)]
        .flatMapConcat {
          case (req, context) =>
            req.entity.withoutSizeLimit().dataBytes.map { body =>
              val uris = Json.decode[List[String]](body.toArray)
              val rewrites = uris.map { uri =>
                response.get(uri) match {
                  case Some(r) => r
                  case None    => Rewrite("NOT_FOUND", uri, uri, "Empty")
                }
              }

              val json =
                if (malformed) Json.encode(rewrites).substring(0, 25) else Json.encode(rewrites)

              val httpResp =
                HttpResponse(
                  status,
                  entity = HttpEntity(ContentTypes.`application/json`, json)
                )
              Success(httpResp) -> context
            }
        }
    }
  }

  class MockLogger extends DataSourceLogger {

    var tuples: List[(DataSource, JsonSupport)] = Nil

    override def apply(ds: DataSource, msg: JsonSupport): Unit = {
      tuples = (ds, msg) :: tuples
    }

    override def close(): Unit = {
      tuples = Nil
    }

    def assertSize(n: Int): Unit = {
      assertEquals(tuples.size, n)
    }
  }
}
