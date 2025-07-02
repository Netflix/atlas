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

import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import java.time.Duration

class DataSourceRewriterSuite extends FunSuite {

  private val dss = DataSources.of(
    new DataSource(
      "foo1",
      Duration.ofSeconds(60),
      "http://localhost/api/v1/graph?q=name,foo,:eq"
    ),
    new DataSource(
      "foo2",
      Duration.ofSeconds(60),
      "http://localhost/api/v1/graph?q=name,foo,:eq&ns=foo"
    ),
    new DataSource(
      "bar1",
      Duration.ofSeconds(60),
      "http://localhost/api/v1/graph?q=name,bar,:eq&ns=bar"
    ),
    new DataSource(
      "bar2",
      Duration.ofSeconds(60),
      "https://localhost/api/v1/graph?q=name,bar,:eq&ns=bar"
    ),
    new DataSource(
      "bar3",
      Duration.ofSeconds(60),
      "https://localhost:12345/api/v1/graph?q=name,bar,:eq&ns=bar"
    ),
    new DataSource(
      "bar4",
      Duration.ofSeconds(5),
      "https://localhost:12345/api/v1/graph?q=name,bar,:eq&ns=bar&tz=UTC"
    ),
    new DataSource(
      "bad",
      Duration.ofSeconds(60),
      "http://localhost/api/v1/graph?q=name,bar,:eq&ns=bad"
    )
  )

  private var config: Config = _
  private var logger: MockLogger = _
  private var ctx: StreamContext = _

  private implicit def system: ActorSystem = ActorSystem(getClass.getSimpleName)

  override def beforeEach(context: BeforeEach): Unit = {
    config = ConfigFactory.load()
    logger = new MockLogger()
    ctx = new StreamContext(config, Materializer(system), dsLogger = logger)
  }

  test("rewrite: invalid") {
    config = ConfigFactory.parseString("""
        |atlas.eval.stream.rewrite {
        |  mode = foo
        |}
        |""".stripMargin)
    val e = intercept[IllegalArgumentException] {
      DataSourceRewriter.create(config)
    }
    assertEquals(e.getMessage, "unsupported rewrite mode: foo")
  }

  test("rewrite: none") {
    config = ConfigFactory.parseString("""
        |atlas.eval.stream.rewrite {
        |  mode = none
        |}
        |""".stripMargin)
    val rewriter = DataSourceRewriter.create(config)
    assertEquals(rewriter.rewrite(ctx, dss), dss)
  }

  test("rewrite: config") {
    config = ConfigFactory.parseString("""
        |atlas.eval.stream.rewrite {
        |  mode = config
        |  namespaces = [
        |    { namespace = "foo", host = "foo.example.com" },
        |    { namespace = "bar", host = "bar.example.com" }
        |  ]
        |}
        |""".stripMargin)
    val rewriter = DataSourceRewriter.create(config)
    val newDss = rewriter.rewrite(ctx, dss)
    assertEquals(newDss.sources.size, dss.sources.size - 1)

    // Bad entry should have been logged
    assertEquals(logger.tuples.size, 1)
    logger.tuples.foreach {
      case (ds, msg: DiagnosticMessage) =>
        assertEquals(ds.id, "bad")
        assertEquals(msg.message, "unsupported namespace: bad")
      case (ds, msg) =>
        fail(s"unexpected message type: $ds: $msg")
    }

    // Other entries should be mapped\
    import scala.jdk.CollectionConverters.*
    val dsMap = dss.sources.asScala.map(ds => ds.id -> ds).toMap
    newDss.sources.forEach { ds =>
      ds.id match {
        case "foo1" =>
          // No explicit parameter, no modification
          assertEquals(ds, dsMap(ds.id))
        case "foo2" =>
          // Basic rewrite
          assertEquals(ds.uri, "http://foo.example.com/api/v1/graph?q=name,foo,:eq")
        case "bar1" =>
          // Basic rewrite, different ns
          assertEquals(ds.uri, "http://bar.example.com/api/v1/graph?q=name,bar,:eq")
        case "bar2" =>
          // Basic rewrite, scheme is not modified
          assertEquals(ds.uri, "https://bar.example.com/api/v1/graph?q=name,bar,:eq")
        case "bar3" =>
          // Basic rewrite, explicit port preserved
          assertEquals(ds.uri, "https://bar.example.com:12345/api/v1/graph?q=name,bar,:eq")
        case "bar4" =>
          // Basic rewrite, other query params preserved
          assertEquals(ds.uri, "https://bar.example.com:12345/api/v1/graph?q=name,bar,:eq&tz=UTC")
      }
      assertEquals(ds.step, dsMap(ds.id).step)
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
