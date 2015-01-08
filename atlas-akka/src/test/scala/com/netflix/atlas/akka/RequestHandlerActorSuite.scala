/*
 * Copyright 2015 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuiteLike
import spray.http.HttpMethods._
import spray.http.StatusCodes._
import spray.http._


class RequestHandlerActorSuite extends TestKit(ActorSystem())
    with ImplicitSender
    with FunSuiteLike
    with BeforeAndAfterAll {

  private val ref = TestActorRef(new RequestHandlerActor)

  override def afterAll() {
    system.shutdown()
  }

  test("/healthcheck") {
    ref ! HttpRequest(GET, Uri("/healthcheck"))
    expectMsgPF() {
      case HttpResponse(OK, _, _, _) =>
    }
  }

  test("cors preflight") {
    ref ! HttpRequest(OPTIONS, Uri("/api/v2/ip"))
    expectMsgPF() {
      case HttpResponse(OK, _, _, _) =>
    }
  }
}
