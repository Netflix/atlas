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
package com.netflix.atlas.akka

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Utils
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success

class StreamOpsSuite extends AnyFunSuite {

  private implicit val ec = scala.concurrent.ExecutionContext.global

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val materializer = ActorMaterializer()

  private def checkOfferedCounts(registry: Registry, expected: Map[String, Double]): Unit = {
    import scala.jdk.CollectionConverters._
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

  test("blocking queue, enqueued") {
    val registry = new DefaultRegistry()
    val source = StreamOps.blockingQueue[Int](registry, "test", 10)
    val queue = source.toMat(Sink.ignore)(Keep.left).run()
    Seq(1, 2, 3, 4).foreach(queue.offer)
    queue.complete()
    checkOfferedCounts(registry, Map("enqueued" -> 4.0))
  }

  test("blocking queue, droppedQueueFull") {
    val registry = new DefaultRegistry()
    val source = StreamOps.blockingQueue[Future[Int]](registry, "test", 1)
    val streamStarted = new CountDownLatch(1)
    val queue = source
      .flatMapConcat(Source.fromFuture)
      .map { value =>
        streamStarted.countDown()
        value
      }
      .toMat(Sink.ignore)(Keep.left)
      .run()

    // wait for stream to start and first item to pass through
    queue.offer(Promise.successful(0).future)
    streamStarted.await()

    val promise = Promise[Int]()
    queue.offer(promise.future) // will pass through without going to the queue
    queue.offer(promise.future) // fills the 1 slot in the queue
    Seq(2, 3, 4, 5).foreach(i => queue.offer(Future(i)))
    promise.complete(Success(1))
    queue.complete()
    checkOfferedCounts(registry, Map("enqueued" -> 3.0, "droppedQueueFull" -> 4.0))
  }

  test("blocking queue, droppedQueueClosed") {
    val registry = new DefaultRegistry()
    val source = StreamOps.blockingQueue[Int](registry, "test", 1)
    val queue = source
      .toMat(Sink.ignore)(Keep.left)
      .run()
    queue.offer(1)
    queue.complete()
    Seq(2, 3, 4, 5).foreach(i => queue.offer(i))
    checkOfferedCounts(registry, Map("enqueued" -> 1.0, "droppedQueueClosed" -> 4.0))
  }

  test("blocking queue, complete with no data") {
    val registry = new DefaultRegistry()
    val source = StreamOps.blockingQueue[Int](registry, "test", 1)
    val latch = new CountDownLatch(1)
    val (queue, fut) = source
      .toMat(Sink.foreach(_ => latch.countDown()))(Keep.both)
      .run()
    queue.offer(1)
    latch.await()
    queue.complete()
    Await.ready(fut, Duration.Inf)
  }

  private def checkCounts(registry: Registry, name: String, expected: Map[String, Double]): Unit = {
    import scala.jdk.CollectionConverters._
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

  private class Message(latch: CountDownLatch, val data: Source[Int, NotUsed]) {

    def dispose(mat: Materializer): Unit = {
      data
        .map { v =>
          latch.countDown()
          v
        }
        .runWith(Sink.ignore)(mat)
    }
  }

  test("map") {
    val latch = new CountDownLatch(100)
    val future = Source(0 until 10)
      .map { v =>
        new Message(latch, Source(0 until 10))
      }
      .via(StreamOps.map { (msg, mat) =>
        msg.dispose(mat)
      })
      .runWith(Sink.ignore)
    Await.result(future, Duration.Inf)
    latch.await(1, TimeUnit.MINUTES)
  }

  test("flatMapConcat") {
    val latch = new CountDownLatch(100)
    val future = Source(0 until 10)
      .map { v =>
        new Message(latch, Source(0 until 10))
      }
      .via(StreamOps.flatMapConcat { (msg, mat) =>
        msg.dispose(mat)
        msg.data
      })
      .runWith(Sink.ignore)
    Await.result(future, Duration.Inf)
    latch.await(1, TimeUnit.MINUTES)
  }

  test("materializer supervision") {
    val registry = new DefaultRegistry()
    val materializer = StreamOps.materializer(system, registry)
    val future = Source
      .single(42)
      .map(_ / 0)
      .runWith(Sink.ignore)(materializer)
    Await.ready(future, Duration.Inf)

    val c = registry.counter("akka.stream.exceptions", "error", "ArithmeticException")
    assert(c.count() === 1)
  }
}
