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
package com.netflix.atlas.pekko

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer
import com.netflix.spectator.atlas.AtlasRegistry
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

@State(Scope.Thread)
class MonitorFlowBench {

  import MonitorFlowBench.*

  private implicit var system: ActorSystem = _
  private var registry: Registry = _

  @Setup
  def setup(): Unit = {
    system = ActorSystem(getClass.getSimpleName)
    registry = new AtlasRegistry(Clock.SYSTEM, k => System.getProperty(k))
  }

  @TearDown
  def tearDown(): Unit = {
    Await.result(system.terminate(), Duration.Inf)
  }

  @Benchmark
  def individual(bh: Blackhole): Unit = {
    val future = Source(0 until 1_000_000)
      .via(new MonitorFlow(registry, "1"))
      .via(new MonitorFlow(registry, "2"))
      .via(new MonitorFlow(registry, "3"))
      .reduce(_ + _)
      .runWith(Sink.head)
    bh.consume(Await.result(future, Duration.Inf))
  }

  @Benchmark
  def batched(bh: Blackhole): Unit = {
    val future = Source(0 until 1_000_000)
      .via(StreamOps.monitorFlow(registry, "1"))
      .via(StreamOps.monitorFlow(registry, "2"))
      .via(StreamOps.monitorFlow(registry, "3"))
      .reduce(_ + _)
      .runWith(Sink.head)
    bh.consume(Await.result(future, Duration.Inf))
  }
}

object MonitorFlowBench {

  private final class MonitorFlow[T](registry: Registry, id: String)
      extends GraphStage[FlowShape[T, T]] {

    private val numEvents = registry.counter("pekko.stream.numEvents", "id", id)
    private val upstreamTimer = registry.timer("pekko.stream.upstreamDelay", "id", id)
    private val downstreamTimer = registry.timer("pekko.stream.downstreamDelay", "id", id)

    private val in = Inlet[T]("MonitorBackpressure.in")
    private val out = Outlet[T]("MonitorBackpressure.out")

    override val shape: FlowShape[T, T] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

      new GraphStageLogic(shape) with InHandler with OutHandler {

        private var upstreamStart: Long = -1L
        private var downstreamStart: Long = -1L

        override def onPush(): Unit = {
          upstreamStart = record(upstreamTimer, upstreamStart)
          push(out, grab(in))
          numEvents.increment()
          downstreamStart = registry.clock().monotonicTime()
        }

        override def onPull(): Unit = {
          downstreamStart = record(downstreamTimer, downstreamStart)
          pull(in)
          upstreamStart = registry.clock().monotonicTime()
        }

        private def record(timer: Timer, start: Long): Long = {
          if (start > 0L) {
            val delay = registry.clock().monotonicTime() - start
            timer.record(delay, TimeUnit.NANOSECONDS)
          }
          -1L
        }

        setHandlers(in, out, this)
      }
    }
  }
}
