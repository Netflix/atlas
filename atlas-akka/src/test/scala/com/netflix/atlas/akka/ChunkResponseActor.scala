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
package com.netflix.atlas.akka

import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage._

class ChunkResponseActor extends ActorPublisher[ChunkStreamPart] {

  var n: Int = 42

  def receive = {
    case "start" =>
      onNext(ChunkStreamPart("start\n"))
      writeChunks()
    case Request(_) =>
      writeChunks()
    case Cancel =>
      context.stop(self)
  }

  def writeChunks(): Unit = {
    while (totalDemand > 0L && n > 0) {
      onNext(ChunkStreamPart(s"$n\n"))
      n -= 1
    }
    if (n == 0) {
      onCompleteThenStop()
    }
  }
}
