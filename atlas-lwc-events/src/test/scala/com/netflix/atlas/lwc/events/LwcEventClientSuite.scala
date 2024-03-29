/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.api.ManualClock
import munit.FunSuite

class LwcEventClientSuite extends FunSuite {

  import LwcEventSuite.*

  private val clock = new ManualClock()
  private val step = 5_000L

  override def beforeEach(context: BeforeEach): Unit = {
    clock.setWallTime(0L)
    clock.setMonotonicTime(0L)
  }

  private val sampleSpan: TestEvent = {
    TestEvent(SortedTagMap("app" -> "www", "node" -> "i-123"), 42L)
  }

  private val sampleLwcEvent: LwcEvent = LwcEvent(sampleSpan, extractSpanValue(sampleSpan))

  test("pass-through") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription("1", 60000, "app,foo,:eq", Subscriptions.Events),
        Subscription("2", 60000, "app,www,:eq", Subscriptions.Events)
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne)
    client.process(sampleLwcEvent)
    assertEquals(
      List("""data: {"id":"2","event":{"tags":{"app":"www","node":"i-123"},"duration":42}}"""),
      output.result()
    )
  }

  test("analytics, basic aggregate") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription("1", step, "app,foo,:eq,:sum", Subscriptions.TimeSeries),
        Subscription("2", step, "app,www,:eq,:sum", Subscriptions.TimeSeries)
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne, clock)
    client.process(sampleLwcEvent)
    clock.setWallTime(step)
    client.process(LwcEvent.HeartbeatLwcEvent(step))
    val vs = output.result()
    assertEquals(vs.size, 2)
  }

  test("analytics, basic aggregate extract value") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription(
          "1",
          step,
          "app,www,:eq,value,duration,:eq,:and,:sum",
          Subscriptions.TimeSeries
        )
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne, clock)
    client.process(sampleLwcEvent)
    clock.setWallTime(step)
    client.process(LwcEvent.HeartbeatLwcEvent(step))
    val vs = output.result()
    assertEquals(vs.size, 1)
    assert(vs.forall(_.contains(""""tags":{"app":"www","value":"duration"}""")))
    assert(vs.forall(_.contains(""""value":8.4""")))
  }

  test("analytics, group by") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription("1", step, "app,foo,:eq,:sum,(,node,),:by", Subscriptions.TimeSeries),
        Subscription("2", step, "app,www,:eq,:sum,(,node,),:by", Subscriptions.TimeSeries)
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne, clock)
    client.process(sampleLwcEvent)
    clock.setWallTime(step)
    client.process(LwcEvent.HeartbeatLwcEvent(step))
    val vs = output.result()
    assertEquals(vs.size, 1)
    assert(vs.forall(_.contains(""""tags":{"app":"www","node":"i-123"}""")))
    assert(vs.forall(_.contains(""""value":0.2""")))
  }

  test("analytics, group by missing key") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription("1", 60000, "app,www,:eq,:sum,(,foo,),:by", Subscriptions.TimeSeries)
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne)
    client.process(sampleLwcEvent)
    assert(output.result().isEmpty)
  }

  test("trace analytics, basic aggregate") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription("1", step, "app,www,:eq", Subscriptions.TraceTimeSeries)
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne, clock)
    client.processTrace(Seq(new TestSpan(sampleSpan)))
    clock.setWallTime(step)
    client.process(LwcEvent.HeartbeatLwcEvent(step))
    val vs = output.result()
    assertEquals(vs.size, 1)
  }
}
