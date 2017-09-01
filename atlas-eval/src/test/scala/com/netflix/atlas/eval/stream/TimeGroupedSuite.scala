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
package com.netflix.atlas.eval.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.netflix.atlas.eval.model.TimeGroup
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class TimeGroupedSuite extends FunSuite {

  import TimeGroupedSuite._

  implicit val system = ActorSystem(getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  private def result(future: Future[List[TimeGroup[Event]]]): List[TimeGroup[Event]] = {
    Await
      .result(future, Duration.Inf)
      .reverse
      .map(g => g.copy(values = g.values.sortWith(_.i < _.i)))
  }

  test("in order list") {
    val data =
      List(Event(10, 1), Event(10, 2), Event(10, 3), Event(20, 1), Event(30, 1), Event(30, 2))

    val future = Source(data)
      .via(new TimeGrouped[Event](2, 10, _.timestamp))
      .runFold(List.empty[TimeGroup[Event]])((acc, g) => g :: acc)

    val groups = result(future)
    assert(
      groups === List(
        TimeGroup(10, List(Event(10, 1), Event(10, 2), Event(10, 3))),
        TimeGroup(20, List(Event(20, 1))),
        TimeGroup(30, List(Event(30, 1), Event(30, 2)))
      )
    )
  }

  test("out of order list") {
    val data =
      List(Event(20, 1), Event(10, 2), Event(10, 3), Event(10, 1), Event(30, 1), Event(30, 2))

    val future = Source(data)
      .via(new TimeGrouped[Event](2, 10, _.timestamp))
      .runFold(List.empty[TimeGroup[Event]])((acc, g) => g :: acc)

    val groups = result(future)
    assert(
      groups === List(
        TimeGroup(10, List(Event(10, 1), Event(10, 2), Event(10, 3))),
        TimeGroup(20, List(Event(20, 1))),
        TimeGroup(30, List(Event(30, 1), Event(30, 2)))
      )
    )
  }

  test("late events dropped") {
    val data = List(
      Event(20, 1),
      Event(10, 2),
      Event(10, 3),
      Event(10, 1),
      Event(30, 1),
      Event(30, 2),
      Event(10, 4) // Dropped, came in late and out of window
    )

    val future = Source(data)
      .via(new TimeGrouped[Event](2, 10, _.timestamp))
      .runFold(List.empty[TimeGroup[Event]])((acc, g) => g :: acc)

    val groups = result(future)
    assert(
      groups === List(
        TimeGroup(10, List(Event(10, 1), Event(10, 2), Event(10, 3))),
        TimeGroup(20, List(Event(20, 1))),
        TimeGroup(30, List(Event(30, 1), Event(30, 2)))
      )
    )
  }
}

object TimeGroupedSuite {

  case class Event(timestamp: Long, i: Int)
}
