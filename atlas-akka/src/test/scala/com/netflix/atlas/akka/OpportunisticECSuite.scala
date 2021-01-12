/*
 * Copyright 2014-2021 Netflix, Inc.
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

import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class OpportunisticECSuite extends AnyFunSuite {

  test("implicit ec available") {
    import OpportunisticEC._
    val future = Future(1).map(_ + 1)
    val result = Await.result(future, Duration.Inf)
    assert(result === 2)
  }
}
