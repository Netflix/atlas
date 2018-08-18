/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.SourceQueueWithComplete
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Utils
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success

class StreamOpsSuite extends FunSuite {

  private implicit val ec = scala.concurrent.ExecutionContext.global

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val materializer = ActorMaterializer()

  private def checkOfferedCounts(registry: Registry, expected: Map[String, Double]): Unit = {
    import scala.collection.JavaConverters._
    registry
      .stream()
      .iterator()
      .asScala
      .flatMap(_.measure().iterator().asScala)
      .filter(m => m.id().name().equals("akka.stream.offeredToQueue"))
      .foreach { m =>
        val result = Utils.getTagValue(m.id(), "result")
        assert(m.value() === expected.getOrElse(result, 0.0), result)
      }
  }

  test("source queue, enqueued") {
    val registry = new DefaultRegistry()
    val source = StreamOps.queue[Int](registry, "test", 10, OverflowStrategy.dropNew)
    val queue = source.toMat(Sink.ignore)(Keep.left).run()
    Await.result(Future.sequence(Seq(1, 2, 3, 4).map(queue.offer)), Duration.Inf)
    queue.complete()
    Await.result(queue.watchCompletion(), Duration.Inf)
    checkOfferedCounts(registry, Map("enqueued" -> 4.0))
  }

  test("source queue, droppedQueueFull") {
    val registry = new DefaultRegistry()
    val source = StreamOps.queue[Future[Int]](registry, "test", 1, OverflowStrategy.dropNew)
    val queue = source
      .flatMapConcat(Source.fromFuture)
      .toMat(Sink.ignore)(Keep.left)
      .run()
    val promise = Promise[Int]()
    queue.offer(promise.future)
    Await.result(Future.sequence(Seq(2, 3, 4, 5).map(i => queue.offer(Future(i)))), Duration.Inf)
    promise.complete(Success(1))
    queue.complete()
    Await.result(queue.watchCompletion(), Duration.Inf)
    checkOfferedCounts(registry, Map("enqueued" -> 2.0, "droppedQueueFull" -> 3.0))
  }

  private def checkCounts(registry: Registry, name: String, expected: Map[String, Double]): Unit = {
    import scala.collection.JavaConverters._
    registry
      .stream()
      .iterator()
      .asScala
      .flatMap(_.measure().iterator().asScala)
      .filter(m => m.id().name().equals(s"akka.stream.$name"))
      .foreach { m =>
        val value = Utils.getTagValue(m.id(), "statistic")
        val stat = if (value == null) "count" else value
        assert(m.value() === expected.getOrElse(stat, 0.0), stat)
      }
  }

  private def testMonitorFlow(name: String, expected: Map[String, Double]): Unit = {
    val clock = new ManualClock()
    val registry = new DefaultRegistry(clock)
    val future = Source(0 until 10)
      .via(StreamOps.monitorFlow(registry, "test"))
      .map(i => clock.setMonotonicTime(i * 3))
      .runWith(Sink.ignore)
    Await.result(future, Duration.Inf)
    checkCounts(registry, name, expected)
  }

  test("monitor flow: number of events") {
    testMonitorFlow("numEvents", Map("count" -> 10.0))
  }

  test("monitor flow: downstream delay") {
    testMonitorFlow("downstreamDelay", Map("count" -> 8.0, "totalTime" -> 24.0))
  }

  test("monitor flow: upstream delay") {
    testMonitorFlow("upstreamDelay", Map("count" -> 8.0, "totalTime" -> 0.0))
  }
}
