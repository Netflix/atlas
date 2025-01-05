/*
 * Copyright 2014-2025 Netflix, Inc.
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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.StreamOps
import com.netflix.spectator.api.NoopRegistry
import munit.FunSuite

import java.util.concurrent.ArrayBlockingQueue
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class StreamSubscriptionManagerSuite extends FunSuite {

  test("queue is completed when unregistered") {
    implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

    val registry = new NoopRegistry
    val sm = new StreamSubscriptionManager(registry)
    val meta = StreamMetadata("id")

    val blockingQueue = new ArrayBlockingQueue[Seq[JsonSupport]](100)
    val (queue, queueSrc) = StreamOps
      .wrapBlockingQueue[Seq[JsonSupport]](registry, "SubscribeApi", blockingQueue, dropNew = false)
      .toMat(Sink.ignore)(Keep.both)
      .run()
    val handler = new QueueHandler(meta, queue)
    sm.register(meta, handler)
    sm.unregister("id")

    assert(!queue.isOpen)
    // Give it a bit of time for the source to complete
    var attempt = 0
    while (!queueSrc.isCompleted && attempt < 10) {
      Thread.sleep(1000)
      attempt += 1
    }
    assert(queueSrc.isCompleted)

    Await.ready(system.terminate(), Duration.Inf)
  }
}
