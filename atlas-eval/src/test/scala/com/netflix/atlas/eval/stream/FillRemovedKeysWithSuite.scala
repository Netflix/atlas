/*
 * Copyright 2014-2020 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FillRemovedKeysWithSuite extends AnyFunSuite {

  implicit val system = ActorSystem(getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  val map1 = Map[String, String]("a" -> "1")
  val map2 = Map[String, String]("b" -> "2")
  val map3 = Map[String, String]("c" -> "3", "d" -> "4")

  test("test fill keys") {

    val future = Source(List(map1, map2, map3))
      .via(new FillRemovedKeysWith[String, String](_ => "?"))
      .runWith(Sink.seq)

    val outputList = Await.result(future, Duration.Inf).toList

    assert(
      outputList ===
        List(Map("a" -> "1"), Map("a" -> "?", "b" -> "2"), Map("b" -> "?", "c" -> "3", "d" -> "4"))
    )
  }
}
