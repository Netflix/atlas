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
package com.netflix.atlas.poller

import akka.actor.Actor

import scala.util.Failure
import scala.util.Try


class TestActor(ref: DataRef) extends Actor {

  // If there is a failure causing a restart, then we need to mark the
  // promise completed so test cases can move forward.
  ref.complete()

  var data: Try[Any] = Failure(new RuntimeException("not set"))

  def receive: Receive = {
    case Messages.Tick => sender() ! ref.get.get
    case v: AnyRef     => ref.receive(v)
  }
}
