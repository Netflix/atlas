/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.webapi

import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.graph.Grapher
import com.netflix.spectator.api.AbstractRegistry
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.DistributionSummary
import com.netflix.spectator.api.Gauge
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Measurement
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Props
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.testkit.scaladsl.TestSink

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class FetchRequestSourceSuite extends FunSuite {

  private val config = ConfigFactory.load()
  private val grapher = Grapher(config)

  /** Db actor that responds with empty data for every request. */
  private class EmptyDb extends Actor {

    def receive: Receive = {
      case GraphApi.DataRequest(ctx, exprs, _) =>
        val data = exprs.map(e => e -> List.empty[TimeSeries]).toMap
        sender() ! GraphApi.DataResponse(ctx.step, data)
    }
  }

  private def disconnects(registry: Registry): Double = {
    registry
      .counters()
      .iterator()
      .asScala
      .filter(_.id().name() == "atlas.webapi.clientDisconnect")
      .map(_.actualCount())
      .sum
  }

  /**
    * Counter that releases a latch once it has been updated. The disconnect is recorded on a
    * stream thread and there is nothing upstream of the recording stage for the test to
    * synchronize on, so the counter itself provides the happens-before edge.
    */
  private class SignalCounter(delegate: Counter, latch: CountDownLatch) extends Counter {

    override def id(): Id = delegate.id()

    override def measure(): java.lang.Iterable[Measurement] = delegate.measure()

    override def hasExpired: Boolean = delegate.hasExpired

    override def actualCount(): Double = delegate.actualCount()

    override def add(amount: Double): Unit = {
      delegate.add(amount)
      latch.countDown()
    }
  }

  /** Registry that releases `latch` when a counter named `name` is updated. */
  private class SignalRegistry(name: String, latch: CountDownLatch)
      extends AbstractRegistry(Clock.SYSTEM) {

    private val delegate = new DefaultRegistry()

    override protected def newCounter(id: Id): Counter = {
      val c = delegate.counter(id)
      if (id.name() == name) new SignalCounter(c, latch) else c
    }

    override protected def newDistributionSummary(id: Id): DistributionSummary =
      delegate.distributionSummary(id)

    override protected def newTimer(id: Id): Timer = delegate.timer(id)

    override protected def newGauge(id: Id): Gauge = delegate.gauge(id)

    override protected def newMaxGauge(id: Id): Gauge = delegate.maxGauge(id)
  }

  private def withFixture(registry: Registry)(
    f: (ActorSystem, Registry, Source[HttpEntity.ChunkStreamPart, ?]) => Unit
  ): Unit = {
    val system = ActorSystem(s"FetchRequestSourceSuite-${System.nanoTime()}")
    try {
      system.actorOf(Props(new EmptyDb), "db")
      val uri = "/api/v2/fetch?q=name,sps,:eq,:sum&s=e-6h&e=now&step=1h"
      val graphCfg = grapher.toGraphConfig(HttpRequest(uri = uri))
      val response = FetchRequestSource.createResponse(system, graphCfg, registry)
      response.entity match {
        case HttpEntity.Chunked(_, source) => f(system, registry, source)
        case other                         => fail(s"expected a chunked entity, got $other")
      }
    } finally {
      Await.ready(system.terminate(), 30.seconds)
    }
  }

  test("no disconnect recorded when the full response is consumed") {
    withFixture(new DefaultRegistry) { (system, registry, source) =>
      implicit val sys: ActorSystem = system
      // Awaiting the sink gives a happens-before edge: the stream has fully terminated,
      // so the counter is stable by the time it is read.
      Await.result(source.runWith(Sink.ignore), 30.seconds)
      assertEquals(disconnects(registry), 0.0)
    }
  }

  test("disconnect recorded when the client goes away") {
    val latch = new CountDownLatch(1)
    withFixture(new SignalRegistry("atlas.webapi.clientDisconnect", latch)) {
      (system, registry, source) =>
        implicit val sys: ActorSystem = system
        // Cancel without requesting anything. With no demand the terminating `close` message
        // cannot be pushed, so the upstream cannot have completed first and the cancellation is
        // unambiguously an abort. Requesting an element first would be racy: for an empty
        // response the close message is the only element, and the source completes as soon as
        // it has been pushed.
        val probe = source.runWith(TestSink[HttpEntity.ChunkStreamPart]())
        probe.cancel()

        // Blocks until the counter has actually been updated, so the read below is ordered
        // after it. The timeout only bounds a failure, it is not a wait for the common case.
        assert(latch.await(10, TimeUnit.SECONDS), "disconnect counter was never updated")
        assertEquals(disconnects(registry), 1.0)
    }
  }

  test("response entity is a chunked SSE stream") {
    withFixture(new DefaultRegistry) { (system, registry, source) =>
      implicit val sys: ActorSystem = system
      val parts = Await.result(source.runWith(Sink.seq), 30.seconds)
      assert(parts.nonEmpty)
      val text = parts.map(_.data().utf8String).mkString
      assert(text.startsWith("data: "), s"unexpected SSE payload: ${text.take(40)}")
      assertEquals(disconnects(registry), 0.0)
    }
  }
}
