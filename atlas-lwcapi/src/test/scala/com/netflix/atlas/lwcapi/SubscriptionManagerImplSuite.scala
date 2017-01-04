/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import akka.actor.{Actor, ActorSystem, Props}
import org.scalatest.FunSuite
import scala.language.postfixOps

class SubscriptionManagerImplSuite() extends FunSuite {
  test("subscribe, unsubscribe, and get work") {
    val system = ActorSystem("HelloSystem")

    val sm = SubscriptionManagerImpl()

    val exp1 = "exp1"
    val exp2 = "exp2"

    val sse1 = "sse1"
    val ref1 = system.actorOf(Props(new TestActor(sse1, sm)), name = "ref1")

    sm.register(sse1, ref1, "foo")

    sm.subscribe(sse1, exp1)
    assert(sm.actorsForExpression(exp1) === Set(ref1))
    assert(sm.subscribersForExpression(exp1) === Set(sse1))

    sm.subscribe(sse1, exp2)
    assert(sm.actorsForExpression(exp2) === Set(ref1))

    sm.unsubscribe(sse1, exp1)
    assert(sm.actorsForExpression(exp1) === Set())

    assert(sm.unregister(sse1) === List(exp2))
    assert(sm.actorsForExpression(exp1) === Set())

    assert(sm.unregister(sse1) === List())
  }

  test("unknown expression or sseIDs do not cause any exceptions") {
    val sm = SubscriptionManagerImpl()

    sm.subscribe("a", "b")
    sm.unsubscribe("c", "d")
    sm.unregister("e")
  }

  class TestActor(sseId: String, subscriptionManager: SubscriptionManagerImpl) extends Actor {
    def receive = {
      case _ =>
    }
  }
}
