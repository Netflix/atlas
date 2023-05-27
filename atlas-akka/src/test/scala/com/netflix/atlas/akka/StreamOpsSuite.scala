/*
 * Copyright 2014-2023 Netflix, Inc.
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
import akka.stream.Materializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Utils
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success

class StreamOpsSuite extends FunSuite {

  import OpportunisticEC._

  private implicit val system = ActorSystem(getClass.getSimpleName)

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
        assertEquals(m.value(), expected.getOrElse(result, 0.0), result)
      }
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
      .flatMapConcat(Source.future)
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

  test("blocking queue, not open after completed") {
    val registry = new DefaultRegistry()
    val source = StreamOps.blockingQueue[Int](registry, "test", 1)
    val queue = source
      .toMat(Sink.ignore)(Keep.left)
      .run()
    assert(queue.isOpen)

    queue.offer(1)
    assert(queue.isOpen)
    checkOfferedCounts(registry, Map("enqueued" -> 1.0, "droppedQueueClosed" -> 0.0))

    queue.complete()
    assert(!queue.isOpen)
    queue.offer(1)
    checkOfferedCounts(registry, Map("enqueued" -> 1.0, "droppedQueueClosed" -> 1.0))
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
        assertEquals(m.value(), expected.getOrElse(stat, 0.0), stat)
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
    testMonitorFlow("downstreamDelay", Map("count" -> 10.0, "totalTime" -> 27.0))
  }

  test("monitor flow: upstream delay") {
    testMonitorFlow("upstreamDelay", Map("count" -> 10.0, "totalTime" -> 0.0))
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

  test("supervision strategy") {
    val registry = new DefaultRegistry()
    val future = Source
      .single(42)
      .map(_ / 0)
      .withAttributes(StreamOps.supervisionStrategy(registry))
      .runWith(Sink.ignore)
    Await.ready(future, Duration.Inf)

    val c = registry.counter("akka.stream.exceptions", "error", "ArithmeticException")
    assertEquals(c.count(), 1L)
  }

  test("unique") {
    val future = Source(List(1, 1, 2, 3, 3, 3, 4, 5, 6, 6, 7, 1))
      .via(StreamOps.unique())
      .runWith(Sink.seq[Int])
    val vs = Await.result(future, Duration.Inf)
    // Only consecutive repeated values are filtered out, so the final 1 should get repeated
    assertEquals(vs, List(1, 2, 3, 4, 5, 6, 7, 1))
  }

  test("unique timeout") {
    val clock = new ManualClock
    clock.setWallTime(0)
    var count = 0

    val future = Source(List(1, 1, 1, 2, 2, 2))
      .map(v => {
        count += 1
        // Set the timestamp same as value count, simulating 1 value per ms
        clock.setWallTime(count)
        v
      })
      .via(StreamOps.unique(1, clock))
      .runWith(Sink.seq[Int])
    val vs = Await.result(future, Duration.Inf)
    assertEquals(vs, List(1, 1, 2, 2))
  }

  test("repeatLastReceived, steady updates") {
    val input = List(1, 1, 2, 3, 3, 3, 4, 5, 6, 6, 7, 1)
    val future = Source(input)
      .via(StreamOps.repeatLastReceived(30.seconds))
      .runWith(Sink.seq[Int])
    val vs = Await.result(future, Duration.Inf)
    assertEquals(vs, input)
  }

  test("repeatLastReceived") {
    val future = Source
      .repeat(1)
      .via(StreamOps.unique())
      .via(StreamOps.repeatLastReceived(1.millis))
      .take(10)
      .runWith(Sink.seq[Int])
    val vs = Await.result(future, Duration.Inf)
    assertEquals(vs, (0 until 10).map(_ => 1).toList)
  }
}
