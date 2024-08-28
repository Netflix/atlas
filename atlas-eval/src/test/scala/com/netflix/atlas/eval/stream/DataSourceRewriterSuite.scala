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
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes
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

  private val dss = DataSources.of(
    new DataSource("foo", Duration.ofSeconds(60), "http://localhost/api/v1/graph?q=name,foo,:eq"),
    new DataSource(
      "bar",
      Duration.ofSeconds(60),
      "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and"
    )
  )

  private var config: Config = _
  private var logger: MockLogger = _
  private var ctx: StreamContext = null

  override implicit def system: ActorSystem = ActorSystem("Test")

  override def beforeEach(context: BeforeEach): Unit = {
    config = ConfigFactory
      .parseString("""
          |atlas.eval.stream.rewrite {
          |  enabled = true
          |  uri = "http://localhost/api/v1/rewrite"
          |}
          |""".stripMargin)
      .withFallback(ConfigFactory.load())
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
      StatusCodes.OK,
      okRewrite()
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

  test("rewrite: Bad URI in datasources") {
    val client = mockClient(
      StatusCodes.OK,
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

  test("rewrite: Malformed response JSON") {
    val client = mockClient(
      StatusCodes.OK,
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

  test("rewrite: 500") {
    val client = mockClient(
      StatusCodes.InternalServerError,
      Map.empty
    )
    val expected = DataSources.of()
    val obtained = rewrite(dss, client)
    assertEquals(obtained, expected)
    ctx.dsLogger.asInstanceOf[MockLogger].assertSize(0)
  }

  test("rewrite: Missing a rewrite") {
    val client = mockClient(
      StatusCodes.OK,
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

  test("rewrite: source changes with good, bad, good") {
    val client = new MockClient(
      List(
        StatusCodes.OK,
        StatusCodes.InternalServerError,
        StatusCodes.OK
      ),
      List(
        okRewrite(true),
        Map.empty,
        okRewrite()
      )
    ).superPool[List[DataSource]]()

    val rewriter = new DataSourceRewriter(config, new NoopRegistry(), system)
    val future = Source
      .fromIterator(() =>
        List(
          DataSources.of(
            new DataSource(
              "foo",
              Duration.ofSeconds(60),
              "http://localhost/api/v1/graph?q=name,foo,:eq"
            )
          ),
          dss,
          dss
        ).iterator
      )
      .via(rewriter.rewrite(client, ctx, true))
      .grouped(3)
      .toMat(Sink.head)(Keep.right)
      .run()
    val res = Await.result(future, 30.seconds)
    assertEquals(res(0), expectedRewrite(true))
    assertEquals(res(1), expectedRewrite(true))
    assertEquals(res(2), expectedRewrite())
  }

  test("rewrite: retry initial flow with 500s") {
    val client = new MockClient(
      List(
        StatusCodes.InternalServerError,
        StatusCodes.InternalServerError,
        StatusCodes.OK
      ),
      List(
        Map.empty,
        Map.empty,
        okRewrite()
      )
    ).superPool[List[DataSource]]()

    val rewriter = new DataSourceRewriter(config, new NoopRegistry(), system)
    val future = Source
      .single(dss)
      .via(rewriter.rewrite(client, ctx, true))
      .grouped(3)
      .toMat(Sink.head)(Keep.right)
      .run()
    val res = Await.result(future, 30.seconds)
    assertEquals(res(0), DataSources.of())
    assertEquals(res(1), expectedRewrite())
  }

  test("rewrite: retry initial flow with 500, exception, ok") {
    val client = new MockClient(
      List(
        StatusCodes.InternalServerError,
        StatusCodes.custom(0, "no conn", "no conn", false, true),
        StatusCodes.OK
      ),
      List(
        Map.empty,
        Map.empty,
        okRewrite()
      )
    ).superPool[List[DataSource]]()

    val rewriter = new DataSourceRewriter(config, new NoopRegistry(), system)
    val future = Source
      .single(dss)
      .via(rewriter.rewrite(client, ctx, true))
      .grouped(3)
      .toMat(Sink.head)(Keep.right)
      .run()
    val res = Await.result(future, 30.seconds)
    assertEquals(res(0), DataSources.of())
    assertEquals(res(1), expectedRewrite())
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
        new MockClient(List(status), List(response), malformed).superPool[List[DataSource]]()
      }
  }

  def rewrite(dss: DataSources, client: SuperPoolClient): DataSources = {
    val rewriter = new DataSourceRewriter(config, new NoopRegistry(), system)
    val future = Source
      .single(dss)
      .via(rewriter.rewrite(client, ctx, true))
      .toMat(Sink.head)(Keep.right)
      .run()
    Await.result(future, 30.seconds)
  }

  def okRewrite(dropSecond: Boolean = false): Map[String, Rewrite] = {
    val builder = Map.newBuilder[String, Rewrite]
    builder +=
      "http://localhost/api/v1/graph?q=name,foo,:eq" -> Rewrite(
        "OK",
        "http://localhost/api/v1/graph?q=name,foo,:eq",
        "http://localhost/api/v1/graph?q=name,foo,:eq",
        ""
      )

    if (!dropSecond) {
      builder += "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and" -> Rewrite(
        "OK",
        "http://atlas-seg.prod.netflix.net/api/v1/graph?q=name,bar,:eq",
        "http://localhost/api/v1/graph?q=nf.ns,seg.prod,:eq,name,bar,:eq,:and",
        ""
      )
    }

    builder.result()
  }

  def expectedRewrite(dropSecond: Boolean = false): DataSources = {
    val ds1 =
      new DataSource("foo", Duration.ofSeconds(60), "http://localhost/api/v1/graph?q=name,foo,:eq")
    val ds2 = new DataSource(
      "bar",
      Duration.ofSeconds(60),
      "http://atlas-seg.prod.netflix.net/api/v1/graph?q=name,bar,:eq"
    )
    if (dropSecond) {
      DataSources.of(ds1)
    } else {
      DataSources.of(ds1, ds2)
    }
  }

  class MockClient(
    status: List[StatusCode],
    response: List[Map[String, Rewrite]],
    malformed: Boolean = false
  ) extends PekkoHttpClient {

    var called = 0

    override def singleRequest(request: HttpRequest): Future[HttpResponse] = {
      Future.failed(new UnsupportedOperationException())
    }

    override def superPool[C](
      config: PekkoHttpClient.ClientConfig
    ): Flow[(HttpRequest, C), (Try[HttpResponse], C), NotUsed] = {
      Flow[(HttpRequest, C)]
        .flatMapConcat {
          case (req, context) =>
            req.entity.withoutSizeLimit().dataBytes.map { body =>
              val httpResp = status(called) match {
                case status if status.intValue() == 0 => null

                case status if status.intValue() != 200 =>
                  HttpResponse(
                    status,
                    entity = HttpEntity(ContentTypes.`application/json`, "{\"error\":\"whoops\"}")
                  )

                case status =>
                  val uris = Json.decode[List[String]](body.toArray)
                  val rewrites = uris.map { uri =>
                    response(called).get(uri) match {
                      case Some(r) => r
                      case None    => Rewrite("NOT_FOUND", uri, uri, "Empty")
                    }
                  }

                  val json =
                    if (malformed) Json.encode(rewrites).substring(0, 25) else Json.encode(rewrites)

                  HttpResponse(
                    status,
                    entity = HttpEntity(ContentTypes.`application/json`, json)
                  )
              }

              called += 1
              if (httpResp == null) {
                Failure(new RuntimeException("no conn")) -> context
              } else {
                Success(httpResp) -> context
              }
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
