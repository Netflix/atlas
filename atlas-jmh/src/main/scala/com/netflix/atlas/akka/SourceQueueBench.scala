/*
 * Copyright 2014-2022 Netflix, Inc.
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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.spectator.api.NoopRegistry
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Double check performance of blocking queue.
  *
  * For more information about problems with Source.queue and Source.actorRef see:
  * https://github.com/akka/akka/issues/25798
  *
  * Custom blocking queue source has much lower overhead in terms of worst-case memory
  * use, number of allocations, and peak throughput. Note though, that if the higher rate
  * is actually hit in practice then most likely a large fraction of the incoming data is
  * going to be dropped.
  *
  * ```
  * > jmh:run -prof stack -wi 10 -i 10 -f1 -t4 .*SourceQueueBench.*
  * ...
  * Benchmark                            Mode  Cnt         Score       Error   Units
  * sourceActorRef                      thrpt   10     76267.028 ± 17961.096   ops/s
  * sourceBlockingQueue                 thrpt   10  21011710.919 ± 25699.817   ops/s
  * sourceQueue                         thrpt   10    230878.311 ±  5346.921   ops/s
  *
  * Benchmark                            Mode  Cnt         Score       Error   Units
  * sourceActorRef         gc.alloc.rate.norm   10        48.063 ±     0.024    B/op
  * sourceBlockingQueue    gc.alloc.rate.norm   10         0.252 ±     0.002    B/op
  * sourceQueue            gc.alloc.rate.norm   10       593.506 ±   107.083    B/op
  *
  * Benchmark                            Mode  Cnt         Score       Error   Units
  * sourceActorRef                   gc.count   10        13.000              counts
  * sourceBlockingQueue              gc.count   10         4.000              counts
  * sourceQueue                      gc.count   10       397.000              counts
  *
  * Benchmark                            Mode  Cnt         Score       Error   Units
  * sourceActorRef                    gc.time   10    165589.000                  ms
  * sourceBlockingQueue               gc.time   10         3.000                  ms
  * sourceQueue                       gc.time   10       314.000                  ms
  * ```
  */
@State(Scope.Benchmark)
class SourceQueueBench {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  private val queue = Source
    .queue[NotUsed](10, OverflowStrategy.dropNew)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  private val actorRef = Source
    .actorRef[NotUsed](10, OverflowStrategy.dropNew)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  private val blockingQueue = StreamOps
    .blockingQueue[NotUsed](new NoopRegistry, "test", 10)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  @TearDown
  def tearDown(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
  }

  @Benchmark
  def sourceQueue(bh: Blackhole): Unit = {
    val result = Await.ready(queue.offer(NotUsed), Duration.Inf)
    bh.consume(result)
  }

  @Benchmark
  def sourceActorRef(): Unit = {
    actorRef ! NotUsed
  }

  @Benchmark
  def sourceBlockingQueue(bh: Blackhole): Unit = {
    bh.consume(blockingQueue.offer(NotUsed))
  }

}
