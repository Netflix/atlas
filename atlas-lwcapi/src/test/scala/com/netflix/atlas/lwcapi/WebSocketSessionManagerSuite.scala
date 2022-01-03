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
package com.netflix.atlas.lwcapi

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.lwcapi.SubscribeApi.ErrorMsg
import munit.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WebSocketSessionManagerSuite extends FunSuite {

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  test("subscribe - one by one") {
    val subscriptionList = List(
      """[{"expression": "name,a,:eq,:sum", "step": 10000}]""",
      """[{"expression": "name,b,:eq", "step": 5000}]"""
    )

    val subsCollector = ArrayBuffer[List[ExpressionMetadata]]()
    val subFunc = createSubscribeFunc(subsCollector)
    val regFun = createNoopRegisterFunc()

    run(subscriptionList, regFun, subFunc)

    assertEquals(
      subsCollector.toList,
      List(
        List(ExpressionMetadata("name,a,:eq,:sum", 10000)),
        List(ExpressionMetadata("name,b,:eq", 5000))
      )
    )
  }

  test("subscribe - ignore bad subscription") {
    val subscriptionList = List(
      """Bad Expression""",
      """[{"expression": "name,b,:eq", "step": 5000}]"""
    )
    val subsCollector = ArrayBuffer[List[ExpressionMetadata]]()
    val subFunc = createSubscribeFunc(subsCollector)
    val regFun = createNoopRegisterFunc()

    run(subscriptionList, regFun, subFunc)

    assertEquals(
      subsCollector.toList,
      List(
        List(ExpressionMetadata("name,b,:eq", 5000))
      )
    )
  }

  private def run(
    data: List[String],
    registerFunc: StreamMetadata => (QueueHandler, Source[JsonSupport, Unit]),
    subscribeFunc: (String, List[ExpressionMetadata]) => List[ErrorMsg]
  ): List[String] = {
    val future = Source(data)
      .via(new WebSocketSessionManager(StreamMetadata(""), registerFunc, subscribeFunc))
      .flatMapMerge(Int.MaxValue, source => source)
      .map(_.toJson)
      .runWith(Sink.seq)

    Await.result(future, Duration.Inf).toList
  }

  private def createSubscribeFunc(
    subsCollector: ArrayBuffer[List[ExpressionMetadata]]
  ): (String, List[ExpressionMetadata]) => List[ErrorMsg] = {
    val subFunc = (_: String, expressions: List[ExpressionMetadata]) => {
      subsCollector += expressions
      List[ErrorMsg]()
    }
    subFunc
  }

  private def createNoopRegisterFunc()
    : StreamMetadata => (QueueHandler, Source[JsonSupport, Unit]) = {
    val noopQueueHandler = new QueueHandler(StreamMetadata(""), null) {
      override def offer(msgs: Seq[JsonSupport]): Unit = ()
      override def complete(): Unit = ()
    }
    val noopSource = Source.empty[JsonSupport].mapMaterializedValue(_ => ())
    val noopRegisterFunc = (_: StreamMetadata) => (noopQueueHandler, noopSource)

    noopRegisterFunc
  }
}
