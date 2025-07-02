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

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcDiagnosticMessage
import com.netflix.atlas.eval.model.LwcEvent
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.spectator.api.DefaultRegistry
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import munit.FunSuite
import org.apache.pekko.util.ByteString

import java.nio.file.Path
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.util.Success
import scala.util.Using

class EvaluatorSuite extends FunSuite {

  private val targetDir = Paths.get(SrcPath.forProject("atlas-eval"), "target", "EvaluatorSuite")

  private val resourcesDir =
    Paths.get(SrcPath.forProject("atlas-eval"), "src", "test", "resources")

  private val config = ConfigFactory.load()
  private val registry = new DefaultRegistry()
  private implicit val system: ActorSystem = ActorSystem("test", config)

  override def beforeEach(context: BeforeEach): Unit = {
    Files.createDirectories(targetDir)
  }

  def testPublisher(baseUri: String, bufferSize: Option[Int] = None): Unit = {
    val buffers = bufferSize.getOrElse(config.getInt("atlas.eval.stream.num-buffers"))
    val evaluator = new Evaluator(
      config.withValue("atlas.eval.stream.num-buffers", ConfigValueFactory.fromAnyRef(buffers)),
      registry,
      system
    )

    val uri = s"$baseUri?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    val future = Source
      .fromPublisher(evaluator.createPublisher(uri))
      .toMat(Sink.seq[JsonSupport])(Keep.right)
      .run()

    val messages = Await.result(future, 1.minute).collect {
      case t: TimeSeriesMessage => t
    }

    val expectedMessages = if (buffers > 1) 256 else 255
    assertEquals(
      messages.size,
      expectedMessages
    ) // Can vary depending on num buffers for evaluation
    assertEquals(messages.map(_.tags("nf.asg")).toSet.size, 3)
  }

  test("write to file") {
    val evaluator = new Evaluator(config, registry, system)

    val uri = "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    val path = targetDir.resolve("test.dat")
    val duration = Duration.ofMinutes(1)
    evaluator.writeInputToFile(uri, path, duration)

    val expected = countMessages(resourcesDir.resolve("gc-pause.dat"))
    val actual = countMessages(path)
    assertEquals(expected, actual)
  }

  private def countMessages(path: Path): Map[String, Int] = {
    import scala.jdk.StreamConverters.*
    Using.resource(Files.lines(path)) { lines =>
      lines
        .toScala(List)
        .map(LwcMessages.parse)
        .flatMap {
          case m: LwcExpression        => Option(m.`type`)
          case m: LwcSubscription      => Option(m.`type`)
          case m: LwcDatapoint         => Option(m.`type`)
          case m: LwcEvent             => Option(m.`type`)
          case m: LwcDiagnosticMessage => Option(m.`type`)
          case m: LwcHeartbeat         => Option(m.`type`)
          case _                       => None
        }
        .groupBy(v => v)
        .map(t => t._1 -> t._2.size)
    }
  }

  test("create publisher from file uri") {
    testPublisher(s"$resourcesDir/gc-pause.dat")
  }

  test("create publisher from resource uri") {
    testPublisher("resource:///gc-pause.dat")
  }

  test("create publisher from resource uri with higher number of time buffers") {
    // has enough buffers to retain an AggrDatapoint with an older timestamp than an earlier AggrDatapoint processed.
    testPublisher("resource:///gc-pause.dat", Some(2))
  }

  test("create publish, missing q parameter") {
    val evaluator = new Evaluator(config, registry, system)

    val uri = "resource:///gc-pause.dat/api/v1/graph"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    assertEquals(result.size, 1)
    result.foreach {
      case DiagnosticMessage(t, msg, None) =>
        assertEquals(t, "error")
        assertEquals(
          msg,
          "IllegalArgumentException: missing required parameter 'q'"
        )
      case v =>
        throw new MatchError(v)
    }
  }

  test("create publish, for an expression with incoming data points exceeding the limit") {
    val evaluator = new Evaluator(
      config.withValue(
        "atlas.eval.stream.limits.max-input-datapoints",
        ConfigValueFactory.fromAnyRef(10)
      ),
      registry,
      system
    )
    val uri = "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    var numberOfDiagnosticMessages = 0
    var numberOfTimeSeriesMessages = 0
    result.foreach {
      case DiagnosticMessage(t, msg, None) =>
        assertEquals(t, "error")
        assert(
          msg.startsWith(
            "expression: statistic,max,:eq,name,jvm.gc.pause,:eq,:and,:max,(,nf.asg,nf.node,),:by" +
              " exceeded the configured max input datapoints limit '10' or max intermediate" +
              " datapoints limit '2147483647' for timestamp"
          )
        )
        numberOfDiagnosticMessages += 1
      case timeSeriesMessage: TimeSeriesMessage =>
        assertEquals(timeSeriesMessage.label, "NO DATA")
        numberOfTimeSeriesMessages += 1
      case _ =>
    }

    assertEquals(
      numberOfDiagnosticMessages,
      9
    ) // 1 diagnostic message per unique timestamp and expression.
    assertEquals(numberOfTimeSeriesMessages, 9) // 1 time series messages with "NO DATA".
  }

  private def ds(id: String, uri: String, step: Long = 60): Evaluator.DataSource = {
    new Evaluator.DataSource(id, java.time.Duration.ofSeconds(step), uri)
  }

  def testProcessor(baseUri: String): Unit = {
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
        .concat(Source.future(p2.future))
        .concat(Source.future(p3.future))
        .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
    )

    // States
    var state = 0
    val states = Array[(AtomicInteger, AtomicInteger) => Unit](
      // 0: Start getting events for one only
      (one, two) => {
        if (one.getAndSet(0) > 0L) {
          assertEquals(two.getAndSet(0), 0)
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
      msg.id match {
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
    result.message match {
      case DiagnosticMessage(t, msg, None) =>
        assertEquals(t, "error")
        assertEquals(msg, expectedMsg)
      case v =>
        throw new MatchError(v)
    }
  }

  test("create processor, missing q parameter") {
    val uri = "synthetic://test/api/v1/graph"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    val msg =
      "IllegalArgumentException: missing required parameter 'q'"
    testError(ds1, msg)
  }

  test("create processor, expression uses :offset") {
    val expr = "name,foo,:eq,:sum,PT168H,:offset"
    val uri = s"synthetic://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: :offset not supported for streaming evaluation [[$expr]]"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, expression uses style variant of :offset") {
    val expr = "name,foo,:eq,:sum,(,0h,1w,),:offset"
    val badExpr = "name,foo,:eq,:sum,PT168H,:offset"
    val uri = s"synthetic://test/api/v1/graph?q=$expr"
    val msg =
      s"IllegalArgumentException: :offset not supported for streaming evaluation [[$badExpr]]"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, reject expensive :in queries") {
    val expr = "name,(,1,2,3,4,5,6,),:in,:sum"
    val uri = s"synthetic://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: rejected expensive query [name,(,1,2,3,4,5,6,),:in], " +
      "narrow the scope to a specific app or name"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, reject expensive :re queries") {
    val expr = "name,foo,:re,:sum"
    val uri = s"synthetic://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: rejected expensive query [name,foo,:re], " +
      "narrow the scope to a specific app or name"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  test("create processor, ignored tag keys are honored") {
    val query = "name,foo,:re,nf.account,12345,:eq,:and"
    val expr = s"$query,:sum"
    val uri = s"synthetic://test/api/v1/graph?q=$expr"
    val msg = s"IllegalArgumentException: rejected expensive query [$query], " +
      "narrow the scope to a specific app or name"
    val ds1 = Evaluator.DataSources.of(ds("one", uri))
    testError(ds1, msg)
  }

  // TODO - these datasources changes tests are ignored as they are currently relying
  // on a sleep since making it deterministic will be a challenge. But they do show how
  // the last datasources wins. Previous streams are stopped.
  def dataSourcesChanges(state: Int): Unit = {
    val evaluator = new Evaluator(config, registry, system)

    val baseUri = "resource:///gc-pause.dat"
    val uri = s"$baseUri?q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by"

    def getSources: List[DataSources] = {
      state match {
        // duplicates
        case 0 =>
          List(
            Evaluator.DataSources.of(ds("one", uri), ds("two", uri)),
            Evaluator.DataSources.of(ds("one", uri), ds("two", uri))
          )
        // disjoint
        case 1 =>
          List(
            Evaluator.DataSources.of(ds("one", uri)),
            Evaluator.DataSources.of(ds("two", uri))
          )
        // overlap
        case 2 =>
          List(
            Evaluator.DataSources.of(ds("one", uri)),
            Evaluator.DataSources.of(ds("one", uri), ds("two", uri))
          )
        case x =>
          throw new IllegalArgumentException(s"Haven't setup a test for ${x}")
      }
    }
    val sourceRef = EvaluationFlows.stoppableSource(
      Source
        .fromIterator(() => getSources.iterator)
        .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
    )

    val oneCount = new AtomicInteger()
    val twoCount = new AtomicInteger()
    val sink = Sink.foreach[Evaluator.MessageEnvelope] { msg =>
      if (msg.message().isInstanceOf[TimeSeriesMessage]) {
        msg.id match {
          case "one" => oneCount.incrementAndGet()
          case "two" => twoCount.incrementAndGet()
        }
      }
    }

    val future = sourceRef.source
      .toMat(sink)(Keep.right)
      .run()

    // TODO - We should have a better trigger. Can't trigger off DPs as if duplication breaks,
    // we don't want to stop at 264 and miss dupes.
    Thread.sleep(5000)
    sourceRef.stop()

    Await.result(future, 1.minute)
    // NOTE: Last source wins. So in this case, one is never processed.
    assertEquals(oneCount.get(), if (state == 1) 0 else 255)
    assertEquals(twoCount.get(), 255)
  }

  test("datasources changes, duplicates".ignore) {
    dataSourcesChanges(0)
  }

  test("datasources changes, disjoint".ignore) {
    dataSourcesChanges(1)
  }

  test("datasources changes, overlap".ignore) {
    dataSourcesChanges(2)
  }

  test("processor handles multiple steps") {
    val evaluator = new Evaluator(config, registry, system)

    val ds1 = Evaluator.DataSources.of(
      ds("one", "resource:///05s.dat?q=name,jvm.gc.allocationRate,:eq,nf.app,foo,:eq,:and,:sum", 5),
      ds("two", "resource:///60s.dat?q=name,jvm.gc.allocationRate,:eq,nf.app,foo,:eq,:and,:sum", 60)
    )

    val future = Source
      .single(ds1)
      .via(Flow.fromProcessor(() => evaluator.createStreamsProcessor()))
      .runWith(Sink.seq[MessageEnvelope])

    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)

    val dsOne = result
      .filter(env => env.id == "one" && env.message.isInstanceOf[TimeSeriesMessage])
      .map(_.message.asInstanceOf[TimeSeriesMessage])
    assert(dsOne.forall(_.step == 5000))
    assertEquals(dsOne.size, 120)

    val dsTwo = result
      .filter(env => env.id == "two" && env.message.isInstanceOf[TimeSeriesMessage])
      .map(_.message.asInstanceOf[TimeSeriesMessage])
    assert(dsTwo.forall(_.step == 60000))
    assertEquals(dsTwo.size, 10)
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
    assertEquals(actual, expected)
  }

  test("DataSources encode and decode") {
    val expected = Evaluator.DataSources.of(
      ds("id1", "uri1"),
      ds("id2", "uri2")
    )
    val actual = Json.decode[Evaluator.DataSources](Json.encode(expected))
    assertEquals(actual, expected)
  }

  private def newDataSource(stepParam: Option[String]): Evaluator.DataSource = {
    val uri = "/api/v1/graph?q=a,b,:eq" + stepParam.map(s => s"&step=$s").getOrElse("")
    new Evaluator.DataSource("_", uri)
  }

  test("extractStepFromUri, step param not set") {
    val ds = newDataSource(None)
    assertEquals(ds.step, Duration.ofMinutes(1))
  }

  test("extractStepFromUri, 5s step") {
    val ds = newDataSource(Some("5s"))
    assertEquals(ds.step, Duration.ofSeconds(5))
  }

  test("extractStepFromUri, invalid step") {
    val ds = newDataSource(Some("abc"))
    assertEquals(ds.step, Duration.ofMinutes(1))
  }

  private def validateOk(params: String, path: String = "graph"): Unit = {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      s"synthetic://test/$path?$params"
    )
    evaluator.validate(ds)
  }

  test("validate: ok") {
    validateOk("q=name,jvm.gc.pause,:eq,:dist-max,(,nf.asg,nf.node,),:by")
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
    assertEquals(e.getMessage, "unknown word ':b'")
  }

  private def invalidOperator(op: String, expr: String): Unit = {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      Duration.ofMinutes(1),
      s"resource:///gc-pause.dat?q=$expr"
    )
    val e = intercept[IllegalArgumentException] {
      evaluator.validate(ds)
    }
    assert(e.getMessage.startsWith(s":$op not supported for streaming evaluation "))
  }

  test("validate: unsupported operation `:offset`") {
    invalidOperator("offset", "name,jvm.gc.pause,:eq,:sum,1w,:offset")
  }

  test("validate: unsupported operation `:offset` with math") {
    invalidOperator("offset", "name,a,:eq,:sum,:dup,1w,:offset,:div")
  }

  test("validate: unsupported operation `:offset` with named rewrite and math") {
    invalidOperator("offset", "name,a,:eq,:sum,:sdes-slower,:dup,1w,:offset,:div")
  }

  test("validate: unsupported operation `:integral`") {
    invalidOperator("integral", "name,jvm.gc.pause,:eq,:sum,:integral")
  }

  test("validate: unsupported operation `:filter`") {
    invalidOperator("filter", "name,jvm.gc.pause,:eq,:sum,:stat-max,5,:gt,:filter")
  }

  test("validate: unsupported operation `:topk`") {
    invalidOperator("topk", "name,jvm.gc.pause,:eq,:sum,max,5,:topk")
  }

  test("validate: reject large step sizes") {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      "resource:///gc-pause.dat?q=name,jvm.gc.pause,:eq,:sum&step=5m"
    )
    val e = intercept[IllegalArgumentException] {
      evaluator.validate(ds)
    }
    assertEquals(e.getMessage, "max allowed step size exceeded (PT5M > PT1M)")
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
    assertEquals(e.getMessage, "unknownhost.com")
  }

  test("validate: hi-res with eq for name and app") {
    validateOk("q=name,foo,:eq,nf.app,www,:eq,:and,:sum&step=5s")
  }

  test("validate: hi-res with eq for name and cluster") {
    validateOk("q=name,foo,:eq,nf.cluster,www-dev,:eq,:and,:sum&step=5s")
  }

  test("validate: hi-res with eq for name and asg") {
    validateOk("q=name,foo,:eq,nf.asg,www-dev-v000,:eq,:and,:sum&step=5s")
  }

  test("validate: hi-res with in for name") {
    validateOk("q=name,(,foo,bar,),:in,nf.app,www,:eq,:and,:sum&step=5s")
  }

  test("validate: hi-res with in for app") {
    validateOk("q=name,foo,:eq,nf.app,(,www,www2,),:in,:and,:sum&step=5s")
  }

  test("validate: hi-res with in for cluster") {
    validateOk("q=name,foo,:eq,nf.cluster,(,www-dev,www-prod,),:in,:and,:sum&step=5s")
  }

  test("validate: hi-res with in for asg") {
    validateOk("q=name,foo,:eq,nf.asg,(,www-dev-v001,www-dev-v002,),:in,:and,:sum&step=5s")
  }

  test("validate: hi-res with or for cluster") {
    validateOk(
      "q=name,foo,:eq,nf.cluster,www-dev,:eq,nf.cluster,www-prod,:eq,:or,:and,:sum&step=5s"
    )
  }

  test("validate: events raw") {
    validateOk("q=name,foo,:eq,nf.cluster,www-dev,:eq,:and", path = "events")
  }

  test("validate: events table") {
    validateOk("q=name,foo,:eq,nf.cluster,www-dev,:eq,:and,(,value,),:table", path = "events")
  }

  test("validate: traces") {
    validateOk("q=nf.app,www,:eq,nf.app,db,:eq,:child", path = "traces")
  }

  test("validate: trace time series") {
    validateOk(
      "q=app,www,:eq,app,db,:eq,:child,app,db,:eq,:sum,:span-time-series",
      path = "traces/graph"
    )
  }

  test("validate: ok rewrite") {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      s"synthetic://test/?q=name,foo,:eq&ns=foo"
    )
    evaluator.validate(ds)
  }

  test("validate: invalid rewrite") {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      s"synthetic://test/?q=name,foo,:eq&ns=none"
    )
    val e = intercept[IllegalArgumentException] {
      evaluator.validate(ds)
    }
    assertEquals(e.getMessage, "unsupported namespace: none")
  }

  private def invalidHiResQuery(expr: String): Unit = {
    val evaluator = new Evaluator(config, registry, system)
    val ds = new Evaluator.DataSource(
      "test",
      s"resource:///gc-pause.dat?q=$expr&step=5s"
    )
    val e = intercept[IllegalArgumentException] {
      evaluator.validate(ds)
    }
    assertEquals(
      e.getMessage,
      s"rejected expensive query [$expr], hi-res streams must restrict name and nf.app with :eq or :in"
    )
  }

  test("validate: hi-res with regex for name") {
    invalidHiResQuery("name,foo,:re,nf.app,www,:eq,:and")
  }

  test("validate: hi-res with not name") {
    invalidHiResQuery("name,foo,:eq,:not,nf.app,www,:eq,:and")
  }

  test("validate: hi-res with regex for app") {
    invalidHiResQuery("name,foo,:eq,nf.app,www,:re,:and")
  }

  private val datapointStep = Duration.ofMillis(1)

  private def sampleData(numGroups: Int, numDatapoints: Int): List[Evaluator.DatapointGroup] = {
    import scala.jdk.CollectionConverters.*
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
      .map(_.message)
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

    val future = Source(input)
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

    assertEquals(msgs.map(_.id).distinct, List("test"))
    assertEquals(msgs.count(_.message.isInstanceOf[TimeSeriesMessage]), 1)

    val ts = timeSeriesMessages(msgs).head
    assertEquals(ts.tags, Map("atlas.offset" -> "0w", "name" -> "foo"))
    assertEquals(ts.data, ArrayData(Array(expected)))
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

    assertEquals(msgs.map(_.id).distinct, List("test"))
    assertEquals(msgs.count(_.message.isInstanceOf[TimeSeriesMessage]), 10)

    timeSeriesMessages(msgs).foreach { ts =>
      val expected = ts.tags("id").toDouble
      assertEquals(ts.data, ArrayData(Array(expected)))
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

    assertEquals(msgs.map(_.id).distinct.sorted, List("a", "b"))
    assertEquals(msgs.count(_.message.isInstanceOf[TimeSeriesMessage]), 2)

    timeSeriesMessages(msgs).foreach { ts =>
      assertEquals(ts.data, ArrayData(Array(45.0)))
    }
  }

  test("DataSource serde") {
    val ds = new Evaluator.DataSource("_", Duration.ofSeconds(42L), ":true")
    assertEquals(Json.encode(ds), """{"id":"_","step":42000,"uri":":true"}""")
    assertEquals(Json.decode[Evaluator.DataSource](Json.encode(ds)), ds)
  }

  test("DataSource serde, default step") {
    val ds = new Evaluator.DataSource("_", Duration.ofSeconds(60L), ":true")
    val json = """{"id":"_","uri":":true"}"""
    assertEquals(Json.decode[Evaluator.DataSource](json), ds)
  }

  test("publisher, time series") {
    val evaluator = new Evaluator(config, registry, system)

    val uri =
      "synthetic://test/graph?q=name,cpu,:eq,nf.app,foo,:eq,:and,:max&step=1ms&numStepIntervals=5"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    // 5 data points and 5 messages indicating data volumes
    assertEquals(result.size, 10)
  }

  test("publisher, events") {
    val evaluator = new Evaluator(config, registry, system)

    val uri =
      "synthetic://test/events?q=name,cpu,:eq,nf.app,foo,:eq,:and&step=1ms&numStepIntervals=5"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    assertEquals(result.size, 5000)
  }

  test("publisher, events table") {
    val evaluator = new Evaluator(config, registry, system)
    val uri =
      "synthetic://test/events?q=name,cpu,:eq,nf.app,foo,:eq,:and,(,j,),:table&step=1ms&numStepIntervals=5"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    assertEquals(result.size, 5000)
  }

  test("publisher, events sample") {
    val evaluator = new Evaluator(config, registry, system)
    val uri =
      "synthetic://test/events?q=name,cpu,:eq,nf.app,foo,:eq,:and,(,i,),(,j,),:sample&step=1ms&numStepIntervals=5"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    assertEquals(result.size, 5)
    result.foreach {
      case tsm: TimeSeriesMessage =>
        assert(tsm.samples.nonEmpty)
      case msg =>
        fail(s"unexpected message: $msg")
    }
  }

  test("publisher, trace time series") {
    val evaluator = new Evaluator(config, registry, system)

    val uri =
      "synthetic://test/traces/graph?q=name,cpu,:eq,nf.app,foo,:eq,:and&step=1ms&numStepIntervals=5"
    val future = Source.fromPublisher(evaluator.createPublisher(uri)).runWith(Sink.seq)
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf)
    assertEquals(result.size, 10)
  }

  def getMessages(file: String, filter: Option[String] = None): Seq[ByteString] = {
    import scala.jdk.CollectionConverters.*
    Files
      .readAllLines(Paths.get(file))
      .asScala
      .filter(!_.isBlank)
      .filter(line => filter.forall(line.contains))
      .map(ByteString(_))
      .toSeq
  }
}
