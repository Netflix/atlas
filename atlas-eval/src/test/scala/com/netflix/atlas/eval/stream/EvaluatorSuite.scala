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

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.DefaultRegistry
import com.typesafe.config.ConfigFactory
import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.util.Success

class EvaluatorSuite extends AnyFunSuite with BeforeAndAfter {

  private val targetDir = Paths.get(SrcPath.forProject("atlas-eval"), "target", "EvaluatorSuite")
  private val resourcesDir =
    Paths.get(SrcPath.forProject("atlas-eval"), "src", "test", "resources")

  private val config = ConfigFactory.load()
  private val registry = new DefaultRegistry()
  private implicit val system = ActorSystem("test", config)
  private implicit val materializer = ActorMaterializer()

  before {
    Files.createDirectories(targetDir)
  }

  def testPublisher(baseUri: String): Unit = {
    import scala.concurrent.duration._

    val evaluator = new Evaluator(config, registry, system)

    val uri = s"$baseUri?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    val future = Source
      .fromPublisher(evaluator.createPublisher(uri))
      .toMat(Sink.seq[JsonSupport])(Keep.right)
      .run()

    val messages = Await.result(future, 1.minute).collect {
      case t: TimeSeriesMessage => t
    }
    assert(messages.size === 255) // Can vary depending on num buffers for evaluation
    assert(messages.map(_.tags("nf.asg")).toSet.size === 3)
  }

  test("write to file") {
    val evaluator = new Evaluator(config, registry, system)

    val uri = "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    val path = targetDir.resolve("test.dat")
    val duration = Duration.ofMinutes(1)
    evaluator.writeInputToFile(uri, path, duration)

    val expected = Hash.sha1(Files.readAllBytes(resourcesDir.resolve("gc-pause.dat")))
    val actual = Hash.sha1(Files.readAllBytes(path))
    assert(expected === actual)
  }

  test("create publisher from file uri") {
    testPublisher(s"$resourcesDir/gc-pause.dat")
  }

  test("create publisher from resource uri") {
    testPublisher("resource:///gc-pause.dat")
  }

  test("create publish, missing q parameter") {
    val evaluator = new Evaluator(config, registry, system)

    val uri = "resource:///gc-pause.dat/api/v1/graph"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    assert(result.size === 1)
    result.foreach {
      case DiagnosticMessage(t, msg, None) =>
        assert(t === "error")
        assert(
          msg === "IllegalArgumentException: missing required URI parameter `q`: resource:///gc-pause.dat/api/v1/graph"
        )
    }
  }

  private def ds(id: String, uri: String, step: Long = 60): Evaluator.DataSource = {
    new Evaluator.DataSource(id, java.time.Duration.ofSeconds(step), uri)
  }

  def testProcessor(baseUri: String): Unit = {
    import scala.concurrent.duration._

    val evaluator = new Evaluator(config, registry, system)

    val uri = s"$baseUri?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"

    val ds1 = Evaluator.DataSources.of(
      ds("one", uri)
    )

    // Add source 2
    val ds2 = Evaluator.DataSources.of(
      ds("one", uri),
      ds("two", uri)
    )
    val p2 = Promise[Evaluator.DataSources]()

    // Remove source 1
    val ds3 = Evaluator.DataSources.of(
      ds("two", uri)
    )
    val p3 = Promise[Evaluator.DataSources]()

    // Source that emits ds1 and then will emit ds2 and ds3 when the futures
    // are completed by analyzing the results coming into the sink
    val sourceRef = EvaluationFlows.stoppableSource(
      Source
        .single(ds1)
        .concat(Source.fromFuture(p2.future))
        .concat(Source.fromFuture(p3.future))
        .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
    )

    // States
    var state = 0
    val states = Array[(AtomicInteger, AtomicInteger) => Unit](
      // 0: Start getting events for one only
      (one, two) => {
        if (one.getAndSet(0) > 0L) {
          assert(two.getAndSet(0) === 0)
          p2.complete(Success(ds2))
          state = 1
        }
      },
      // 1: See first events for two
      (one, two) => {
        if (two.getAndSet(0) > 0) {
          one.getAndSet(0)
          state = 2
        }
      },
      // 2: Still seeing events for one and two
      (one, two) => {
        if (one.getAndSet(0) > 0 && two.getAndSet(0) > 0) {
          p3.complete(Success(ds3))
          state = 3
        }
      },
      // 3: Stop seeing events for one
      (one, two) => {
        if (one.getAndSet(0) == 0 && two.get() > 100) {
          sourceRef.stop()
        }
      }
    )

    // Sink tracking how many events we receive for each id. We should get a
    // set for just one, then both, then just two
    val oneCount = new AtomicInteger()
    val twoCount = new AtomicInteger()
    val sink = Sink.foreach[Evaluator.MessageEnvelope] { msg =>
      msg.getId match {
        case "one" => oneCount.incrementAndGet()
        case "two" => twoCount.incrementAndGet()
      }
      states(state)(oneCount, twoCount)
    }

    val future = sourceRef.source
      .toMat(sink)(Keep.right)
      .run()

    try Await.result(future, 1.minute)
    catch {
      case _: TimeoutException => fail(s"stuck in state $state")
    }
  }

  test("create processor from resource uri") {
    testProcessor("resource:///gc-pause.dat")
  }

  private def testError(ds: Evaluator.DataSources, expectedMsg: String): Unit = {
    val evaluator = new Evaluator(config, registry, system)
    val future = Source
      .single(ds)
      .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
      .runWith(Sink.head)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    result.getMessage match {
      case DiagnosticMessage(t, msg, None) =>
        assert(t === "error")
        assert(msg === expectedMsg)
    }
  }

  test("create processor, missing q parameter") {
    val uri = "http://test/api/v1/graph"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    val msg =
      "IllegalArgumentException: missing required URI parameter `q`: http://test/api/v1/graph"
    testError(ds1, msg)
  }

  test("create processor, expression uses :offset") {
    val expr = "name,foo,:eq,:sum,PT168H,:offset"
    val uri = s"http://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: :offset not supported for streaming evaluation [[$expr]]"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, expression uses style variant of :offset") {
    val expr = "name,foo,:eq,:sum,(,0h,1w,),:offset"
    val badExpr = "name,foo,:eq,:sum,PT168H,:offset"
    val uri = s"http://test/api/v1/graph?q=$expr"
    val msg =
      s"IllegalArgumentException: :offset not supported for streaming evaluation [[$badExpr]]"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, reject expensive :in queries") {
    val expr = "name,(,1,2,3,4,5,6,),:in,:sum"
    val uri = s"http://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: rejected expensive query [name,(,1,2,3,4,5,6,),:in], " +
      "narrow the scope to a specific app or name"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, reject expensive :re queries") {
    val expr = "name,foo,:re,:sum"
    val uri = s"http://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: rejected expensive query [name,foo,:re], " +
      "narrow the scope to a specific app or name"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, ignored tag keys are honored") {
    val query = "name,foo,:re,nf.account,12345,:eq,:and"
    val expr = s"$query,:sum"
    val uri = s"http://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: rejected expensive query [$query], " +
      "narrow the scope to a specific app or name"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("processor handles multiple steps") {
    val evaluator = new Evaluator(config, registry, system)

    val ds1 = Evaluator.DataSources.of(
      ds("one", "resource:///05s.dat?q=name,jvm.gc.allocationRate,:eq,:sum", 5),
      ds("two", "resource:///60s.dat?q=name,jvm.gc.allocationRate,:eq,:sum", 60)
    )

    val future = Source
      .single(ds1)
      .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
      .runWith(Sink.seq[MessageEnvelope])

    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)

    val dsOne = result
      .filter(env => env.getId == "one" && env.getMessage.isInstanceOf[TimeSeriesMessage])
      .map(_.getMessage.asInstanceOf[TimeSeriesMessage])
    assert(dsOne.forall(_.step === 5000))
    assert(dsOne.size === 120)

    val dsTwo = result
      .filter(env => env.getId == "two" && env.getMessage.isInstanceOf[TimeSeriesMessage])
      .map(_.getMessage.asInstanceOf[TimeSeriesMessage])
    assert(dsTwo.forall(_.step === 60000))
    assert(dsTwo.size === 10)
  }

  test("DataSource equals contract") {
    EqualsVerifier
      .forClass(classOf[Evaluator.DataSource])
      .suppress(Warning.NULL_FIELDS)
      .verify()
  }

  test("DataSources equals contract") {
    EqualsVerifier
      .forClass(classOf[Evaluator.DataSources])
      .suppress(Warning.NULL_FIELDS)
      .verify()
  }

  test("MessageEnvelope equals contract") {
    EqualsVerifier
      .forClass(classOf[Evaluator.MessageEnvelope])
      .suppress(Warning.NULL_FIELDS)
      .verify()
  }

  test("Datapoint equals contract") {
    EqualsVerifier
      .forClass(classOf[Evaluator.Datapoint])
      .suppress(Warning.NULL_FIELDS)
      .verify()
  }

  test("DatapointGroup equals contract") {
    EqualsVerifier
      .forClass(classOf[Evaluator.DatapointGroup])
      .suppress(Warning.NULL_FIELDS)
      .verify()
  }

  test("DataSource encode and decode") {
    val expected = ds("id", "uri")
    val actual = Json.decode[Evaluator.DataSource](Json.encode(expected))
    assert(actual === expected)
  }

  test("DataSources encode and decode") {
    val expected = Evaluator.DataSources.of(
      ds("id1", "uri1"),
      ds("id2", "uri2")
    )
    val actual = Json.decode[Evaluator.DataSources](Json.encode(expected))
    assert(actual === expected)
  }

  private def newDataSource(stepParam: Option[String]): Evaluator.DataSource = {
    val uri = "/api/v1/graph?q=a,b,:eq" + stepParam.map(s => s"&step=$s").getOrElse("")
    new Evaluator.DataSource("_", uri)
  }

  test("extractStepFromUri, step param not set") {
    val ds = newDataSource(None)
    assert(ds.getStep === Duration.ofMinutes(1))
  }

  test("extractStepFromUri, 5s step") {
    val ds = newDataSource(Some("5s"))
    assert(ds.getStep === Duration.ofSeconds(5))
  }

  test("extractStepFromUri, invalid step") {
    val ds = newDataSource(Some("abc"))
    assert(ds.getStep === Duration.ofMinutes(1))
  }

  test("validate: ok") {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      Duration.ofMinutes(1),
      "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    )
    evaluator.validate(ds)
  }

  test("validate: bad expression") {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      Duration.ofMinutes(1),
      "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:b"
    )
    val e = intercept[IllegalStateException] {
      evaluator.validate(ds)
    }
    assert(e.getMessage === "unknown word ':b'")
  }

  test("validate: unsupported operation `:offset`") {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      Duration.ofMinutes(1),
      "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:sum,1w,:offset"
    )
    val e = intercept[IllegalArgumentException] {
      evaluator.validate(ds)
    }
    assert(e.getMessage.startsWith(":offset not supported for streaming evaluation "))
  }

  test("validate: unknown backend") {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      Duration.ofMinutes(1),
      "http://unknownhost.com?q=name,jvm.gc.pause,:eq,:sum"
    )
    val e = intercept[NoSuchElementException] {
      evaluator.validate(ds)
    }
    assert(e.getMessage === "unknownhost.com")
  }

  private val datapointStep = Duration.ofMillis(1)

  private def sampleData(numGroups: Int, numDatapoints: Int): List[Evaluator.DatapointGroup] = {
    import scala.jdk.CollectionConverters._
    (0 until numGroups).map { i =>
      val ds = (0 until numDatapoints)
        .map { j =>
          val tags = Map("name" -> "foo", "id" -> j.toString).asJava
          new Evaluator.Datapoint(tags, j.toDouble)
        }
        .toList
        .asJava
      new Evaluator.DatapointGroup(i, ds)
    }.toList
  }

  private def timeSeriesMessages(msgs: Seq[Evaluator.MessageEnvelope]): List[TimeSeriesMessage] = {
    msgs
      .map(_.getMessage)
      .collect {
        case ts: TimeSeriesMessage => ts
      }
      .toList
  }

  private def runDatapointFlow(
    sources: Evaluator.DataSources,
    input: List[Evaluator.DatapointGroup]
  ): List[Evaluator.MessageEnvelope] = {

    val evaluator = new Evaluator(config, registry, system)

    val future = Source(sampleData(1, 10))
      .via(Flow.fromProcessor(() => evaluator.createDatapointProcessor(sources)))
      .runWith(Sink.seq[MessageEnvelope])
    Await.result(future, scala.concurrent.duration.Duration.Inf).toList
  }

  private def basicAggregationTest(af: String, expected: Double): Unit = {
    val ds = new Evaluator.DataSource(
      "test",
      datapointStep,
      s"http://localhost/api/v1/graph?q=name,foo,:eq,:$af"
    )
    val sources = Evaluator.DataSources.of(ds)

    val msgs = runDatapointFlow(sources, sampleData(1, 10))

    assert(msgs.map(_.getId).distinct === List("test"))
    assert(msgs.count(_.getMessage.isInstanceOf[TimeSeriesMessage]) === 1)

    val ts = timeSeriesMessages(msgs).head
    assert(ts.tags === Map("name" -> "foo"))
    assert(ts.data === ArrayData(Array(expected)))
  }

  test("datapoint flow: sum") {
    basicAggregationTest("sum", 45.0)
  }

  test("datapoint flow: count") {
    basicAggregationTest("count", 10.0)
  }

  test("datapoint flow: avg") {
    basicAggregationTest("avg", 4.5)
  }

  test("datapoint flow: max") {
    basicAggregationTest("max", 9.0)
  }

  test("datapoint flow: min") {
    basicAggregationTest("min", 0.0)
  }

  test("datapoint flow: grouping") {
    val ds = new Evaluator.DataSource(
      "test",
      datapointStep,
      "http://localhost/api/v1/graph?q=name,foo,:eq,:sum,(,id,),:by"
    )
    val sources = Evaluator.DataSources.of(ds)

    val msgs = runDatapointFlow(sources, sampleData(1, 10))

    assert(msgs.map(_.getId).distinct === List("test"))
    assert(msgs.count(_.getMessage.isInstanceOf[TimeSeriesMessage]) === 10)

    timeSeriesMessages(msgs).foreach { ts =>
      val expected = ts.tags("id").toDouble
      assert(ts.data === ArrayData(Array(expected)))
    }
  }

  test("datapoint flow: multiple subs for same id") {
    val ds1 = new Evaluator.DataSource(
      "a",
      datapointStep,
      "http://localhost/api/v1/graph?q=name,foo,:eq,:sum"
    )
    val ds2 = new Evaluator.DataSource(
      "b",
      datapointStep,
      "http://localhost/api/v1/graph?q=name,foo,:eq,:sum"
    )
    val sources = Evaluator.DataSources.of(ds1, ds2)

    val msgs = runDatapointFlow(sources, sampleData(1, 10))

    assert(msgs.map(_.getId).distinct === List("a", "b"))
    assert(msgs.count(_.getMessage.isInstanceOf[TimeSeriesMessage]) === 2)

    timeSeriesMessages(msgs).foreach { ts =>
      assert(ts.data === ArrayData(Array(45.0)))
    }
  }
}
