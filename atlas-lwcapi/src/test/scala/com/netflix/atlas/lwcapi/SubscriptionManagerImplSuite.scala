/*
 * Copyright 2014-2016 Netflix, Inc.
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

import scala.concurrent.duration._
import akka.actor.{Actor, ActorSystem, Props}
import akka.util.Timeout
import org.scalatest.FunSuite

import scala.concurrent.Await

class SubscriptionManagerImplSuite() extends FunSuite {
  @volatile var lockish: Boolean = false

  test("subscribe, unsubscribe, and get work") {
    val system = ActorSystem("HelloSystem")

    val sm = SubscriptionManagerImpl()

    implicit val timeout = Timeout(5 seconds) // needed for `?` below

    val exp1 = "exp1"
    val exp2 = "exp2"
    val exp3 = "exp3"

    val sse1 = "sse1"
    val ref1 = system.actorOf(Props(new TestActor(sse1, sm)), name = "ref1")

    val sse2 = "sse2"
    val ref2 = system.actorOf(Props(new TestActor(sse2, sm)), name = "ref2")

    sm.subscribe(exp1, sse1, ref1)
    assert(sm.getActorsForExpressionId(exp1) === Set(ref1))
    assert(sm.getExpressionsForSSEId(sse1) === Set(exp1))

    sm.subscribe(exp1, sse2, ref2)
    assert(sm.getActorsForExpressionId(exp1) === Set(ref1, ref2))
    assert(sm.getExpressionsForSSEId(sse2) === Set(exp1))

    assert(sm.getActorsForExpressionId(exp2) === Set())

    sm.unsubscribe(exp1, sse1, ref1)
    assert(sm.getActorsForExpressionId(exp1) === Set(ref2))
    assert(sm.getExpressionsForSSEId(sse1) === Set())

    sm.unsubscribeAll(sse2, ref2)
    assert(sm.getActorsForExpressionId(exp1) === Set())
    assert(sm.getExpressionsForSSEId(sse2) === Set())

    lockish = false
    var counter = 0
    sm.subscribe(exp3, sse1, ref1)
    ref1 ! "terminate"
    while (!lockish || counter > 100) {
      Thread.sleep(100)
      counter += 1
    }
    assert(sm.getActorsForExpressionId(exp3) === Set())
    assert(sm.getExpressionsForSSEId(sse1) === Set())
  }

  class TestActor(sseId: String, subscriptionManager: SubscriptionManagerImpl) extends Actor {
    def receive = {
      case x: String =>
        context.stop(self)
    }

    override def postStop() = {
      subscriptionManager.unsubscribeAll(sseId, self)
      lockish = true
      super.postStop()
    }
  }
}
