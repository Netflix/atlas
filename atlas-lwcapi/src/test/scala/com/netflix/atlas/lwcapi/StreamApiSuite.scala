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

import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.lwcapi.StreamApi._
import org.scalatest.FunSuite

class StreamApiSuite extends FunSuite with ScalatestRouteTest {
  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  //val mockSM = MockSubscriptionManager()
  //val splitter = ExpressionSplitterImpl()
  //val alertmap = AlertMapImpl()
  //val endpoint = new StreamApi(mockSM, splitter, alertmap, system)

  test("SSEHello renders") {
    val uuid = GlobalUUID.get
    val ret = SSEHello("me!me!", "unknown", uuid).toSSE
    assert(ret.contains("info: hello {"))
    assert(ret.contains(""""streamId":"me!me!""""))
    assert(ret.contains(""""instanceUUID":"""))
    assert(ret.contains(""""instanceId":"""))
  }

  test("SSEStatistics renders") {
    assert(SSEStatistics(1).toSSE === """info: statistics {"outputFullFailures":1}""")
  }

  test("SSEShutdown renders") {
    assert(SSEShutdown("foo").toSSE === """info: shutdown {"reason":"foo"}""")
  }

  test("SSESubscribe renders") {
    val ret1 = List(ExpressionWithFrequency("dataExpr", 10, "exprId"))
    val s = SSESubscribe("mainExpr", ret1).toSSE
    assert(s.startsWith("info: subscribe"))
    assert(s.contains(""""expression":"mainExpr""""))
    assert(s.contains(""""metrics":[{"""))
    assert(s.contains(""""expression":"dataExpr""""))
    assert(s.contains(""""frequency":10"""))
    assert(s.contains(""""id":"exprId""""))
  }
}
