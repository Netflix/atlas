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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

class LwcEventClientSuite extends FunSuite {

  import LwcEventSuite.*

  private val sampleSpan: TestEvent = {
    TestEvent(SortedTagMap("app" -> "www", "node" -> "i-123"), 42L)
  }

  private val sampleLwcEvent: LwcEvent = LwcEvent(sampleSpan, extractSpanValue(sampleSpan))

  test("pass-through") {
    val subs = Subscriptions(passThrough =
      List(
        Subscription("1", 60000, "app,foo,:eq"),
        Subscription("2", 60000, "app,www,:eq")
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
    val subs = Subscriptions(analytics =
      List(
        Subscription("1", 60000, "app,foo,:eq,:sum"),
        Subscription("2", 60000, "app,www,:eq,:sum")
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne)
    client.process(sampleLwcEvent)
    val vs = output.result()
    assertEquals(1, vs.size)
    assert(vs.forall(_.contains(""""tags":{"app":"www"}""")))
    assert(vs.forall(_.contains(""""value":1.0""")))
  }

  test("analytics, basic aggregate extract value") {
    val subs = Subscriptions(analytics =
      List(
        Subscription("1", 60000, "app,www,:eq,value,duration,:eq,:and,:sum")
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne)
    client.process(sampleLwcEvent)
    val vs = output.result()
    assertEquals(1, vs.size)
    assert(vs.forall(_.contains(""""tags":{"app":"www","value":"duration"}""")))
    assert(vs.forall(_.contains(""""value":42.0""")))
  }

  test("analytics, group by") {
    val subs = Subscriptions(analytics =
      List(
        Subscription("1", 60000, "app,foo,:eq,:sum,(,node,),:by"),
        Subscription("2", 60000, "app,www,:eq,:sum,(,node,),:by")
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne)
    client.process(sampleLwcEvent)
    val vs = output.result()
    assertEquals(1, vs.size)
    assert(vs.forall(_.contains(""""tags":{"app":"www","node":"i-123"}""")))
    assert(vs.forall(_.contains(""""value":1.0""")))
  }

  test("analytics, group by missing key") {
    val subs = Subscriptions(analytics =
      List(
        Subscription("1", 60000, "app,www,:eq,:sum,(,foo,),:by")
      )
    )
    val output = List.newBuilder[String]
    val client = LwcEventClient(subs, output.addOne)
    client.process(sampleLwcEvent)
    assert(output.result().isEmpty)
  }
}
