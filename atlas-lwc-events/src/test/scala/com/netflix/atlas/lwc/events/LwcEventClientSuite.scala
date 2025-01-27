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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.ManualClock
import munit.FunSuite

import java.io.StringWriter
import scala.util.Using

class LwcEventClientSuite extends FunSuite {

  import LwcEventSuite.*
  import LwcEventClientSuite.*

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
    val client = LwcEventClient(subs, output.addOne, clock)
    client.process(sampleLwcEvent)
    assertEquals(
      List("""data: {"id":"2","event":{"tags":{"app":"www","node":"i-123"},"duration":42}}"""),
      output.result()
    )
  }

  test("sampled pass-through") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription("1", step, "app,foo,:eq,(,app,),(,node,),:sample", Subscriptions.Events),
        Subscription("2", step, "app,www,:eq,(,app,),(,node,),:sample", Subscriptions.Events)
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne, clock)
    client.process(sampleLwcEvent)
    clock.setWallTime(step)
    client.process(LwcEvent.HeartbeatLwcEvent(step))
    assertEquals(
      List(
        """data: {"id":"2","event":{"id":"2","tags":{"app":"www"},"timestamp":5000,"value":0.2,"samples":[["i-123"]]}}"""
      ),
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
    assertEquals(vs.size, 1)
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

  test("analytics, sync") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription("1", step, "app,foo,:eq,:sum", Subscriptions.TimeSeries),
        Subscription("2", step, "app,www,:eq,:sum", Subscriptions.TimeSeries)
      )
    )
    val output = List.newBuilder[String]
    val client = TestLwcEventClient(subs, output.addOne, clock)
    client.process(sampleLwcEvent)
    clock.setWallTime(step)
    client.process(LwcEvent.HeartbeatLwcEvent(step))
    assertEquals(output.result().size, 1)

    // Sync expressions, same set
    (2 until 10).foreach { i =>
      output.clear()
      client.sync(subs)
      client.process(sampleLwcEvent)
      clock.setWallTime(step * i)
      client.process(LwcEvent.HeartbeatLwcEvent(step * i))
      assertEquals(output.result().size, 1)
    }

    // Sync expressions, subset
    (10 until 20).foreach { i =>
      output.clear()
      client.sync(subs.copy(timeSeries = subs.timeSeries.tail))
      client.process(sampleLwcEvent)
      clock.setWallTime(step * i)
      client.process(LwcEvent.HeartbeatLwcEvent(step * i))
      assertEquals(output.result().size, 1)
    }
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

  test("trace analytics, basic aggregate extract value") {
    val subs = Subscriptions.fromTypedList(
      List(
        Subscription(
          "1",
          step,
          "app,www,:eq,value,duration,:eq,:span-time-series",
          Subscriptions.TraceTimeSeries
        )
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne, clock)
    client.processTrace(Seq(new TestSpan(sampleSpan)))
    clock.setWallTime(step)
    client.process(LwcEvent.HeartbeatLwcEvent(step))
    val vs = output.result()
    assertEquals(vs.size, 1)
    assert(vs.forall(_.contains(""""tags":{"value":"duration"}""")))
    assert(vs.forall(_.contains(""""value":8.4""")))
  }

  test("check if event matches query") {
    val matching = Query.And(Query.Equal("app", "www"), Query.Equal("node", "i-123"))
    val nonMatching = Query.And(Query.Equal("app", "www"), Query.Equal("node", "i-124"))
    assert(matching.matches(sampleLwcEvent.tagValue _))
    assert(!nonMatching.matches(sampleLwcEvent.tagValue _))
  }
}

object LwcEventClientSuite {

  case class TestLwcEventClient(
    subscriptions: Subscriptions,
    consumer: String => Unit,
    clock: Clock
  ) extends AbstractLwcEventClient(clock) {

    sync(subscriptions)

    override def sync(subscriptions: Subscriptions): Unit = {
      super.sync(subscriptions)
    }

    override def submit(id: String, event: LwcEvent): Unit = {
      Using.resource(new StringWriter()) { w =>
        Using.resource(Json.newJsonGenerator(w)) { gen =>
          gen.writeStartObject()
          gen.writeStringField("id", id)
          gen.writeFieldName("event")
          event.encode(gen)
          gen.writeEndObject()
        }
        consumer(s"data: ${w.toString}")
      }
    }
  }
}
