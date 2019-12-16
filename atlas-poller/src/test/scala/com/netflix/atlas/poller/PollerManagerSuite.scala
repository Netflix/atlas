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
package com.netflix.atlas.poller

import java.lang.reflect.Type

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import com.netflix.atlas.core.model.Datapoint
import com.netflix.iep.service.DefaultClassFactory
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.Await
import scala.util.Failure
import scala.util.Success

class PollerManagerSuite
    extends TestKit(ActorSystem())
    with ImplicitSender
    with AnyFunSuiteLike
    with BeforeAndAfterAll {

  import scala.concurrent.duration._

  import scala.compat.java8.FunctionConverters._
  private val dataRef = new DataRef
  private val bindings = Map[Type, AnyRef](classOf[DataRef] -> dataRef).withDefaultValue(null)
  private val classFactory = new DefaultClassFactory(bindings.asJava)

  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)
  private val config = ConfigFactory.load()
  private val ref = TestActorRef(new PollerManager(registry, classFactory, config))

  override def afterAll(): Unit = {
    system.terminate()
  }

  private def dataValue: AnyRef = {
    val value = Await.result(dataRef.future, 1.minute)
    dataRef.reset()
    value
  }

  private def waitForCompletion(): Unit = {
    Await.ready(dataRef.future, 1.minute)
    dataRef.reset()
  }

  test("datapoints passed to sink") {
    dataRef.reset()
    dataRef.set(Success(Messages.MetricsPayload()))
    ref ! Messages.Tick
    assert(dataValue === Messages.MetricsPayload())

    val payload =
      Messages.MetricsPayload(Map.empty, List(Datapoint(Map("name" -> "foo"), 0L, 42.0)))

    dataRef.set(Success(payload))
    ref ! Messages.Tick
    assert(dataValue === payload)
  }

  test("datapoints counter incremented") {
    dataRef.reset()
    val datapoints = (0 until 1234)
      .map(i => Datapoint(Map("name" -> "foo"), 0L, i.toDouble))
      .toList
    val payload = Messages.MetricsPayload(Map.empty, datapoints)

    val counter = registry.counter("atlas.poller.datapoints", "id", "poller-test")
    val init = counter.count()

    dataRef.set(Success(payload))
    ref ! Messages.Tick
    assert(dataValue === payload)

    assert(counter.count() === init + datapoints.size)
  }

  test("restarts counter incremented") {
    val e1 = new RuntimeException("foo")
    val e2 = new IllegalArgumentException("bar")

    val c1 =
      registry.counter("atlas.poller.restarts", "id", "poller-test", "error", "RuntimeException")
    val c2 = registry.counter(
      "atlas.poller.restarts",
      "id",
      "poller-test",
      "error",
      "IllegalArgumentException"
    )

    val init1 = c1.count()
    val init2 = c2.count()

    // Update with RuntimeException
    dataRef.reset()
    dataRef.set(Failure(e1))
    ref ! Messages.Tick
    waitForCompletion()
    assert(c1.count() === init1 + 1)
    assert(c2.count() === init2)

    // Update with IllegalArgumentException
    dataRef.reset()
    dataRef.set(Failure(e2))
    ref ! Messages.Tick
    waitForCompletion()
    assert(c1.count() === init1 + 1)
    assert(c2.count() === init2 + 1)
  }

  private def updateGauges(): Unit = {
    // Forces the update of passive gauges
    PolledMeter.update(registry)
  }

  test("age is updated on success") {
    dataRef.reset()
    val payload = Messages.MetricsPayload()
    val m = registry.gauge(registry.createId("atlas.poller.dataAge", "id", "poller-test"))

    val t = clock.wallTime()
    updateGauges()
    assert(m.value() === 0.0)

    clock.setWallTime(t + 60000L)
    dataRef.set(Success(payload))
    ref ! Messages.Tick
    waitForCompletion()
    updateGauges()
    assert(m.value() === 0.0)

  }

  test("age is not updated on failure") {
    dataRef.reset()
    val e1 = new RuntimeException("foo")
    val m = registry.gauge(registry.createId("atlas.poller.dataAge", "id", "poller-test"))

    val t = clock.wallTime()
    updateGauges()
    assert(m.value() === 0.0)

    clock.setWallTime(t + 60000L)
    dataRef.set(Failure(e1))
    ref ! Messages.Tick
    waitForCompletion()
    updateGauges()
    assert(m.value() === 60.0)

  }

}
