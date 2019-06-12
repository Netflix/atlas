/*
 * Copyright 2014-2019 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.NotUsed
import akka.stream.KillSwitches
import akka.stream.Materializer
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.spectator.api.Counter

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

/**
  * Helpers for evaluating Atlas expressions over streaming data sources.
  */
private[stream] object EvaluationFlows {

  /**
    * Run a stream connecting the source to the sink.
    */
  def run[T, M1, M2](source: Source[T, M1], sink: Sink[T, M2])(
    implicit materializer: Materializer
  ): StreamRef[M2] = {

    val (killSwitch, value) = source
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(sink)(Keep.both)
      .run()
    StreamRef(killSwitch, value)
  }

  /**
    * Returns a reference to a source that can be stopped by completing the future. This is
    * intended for sources that will be materialized once for a dynamic source such as a
    * particular instance from a eureka vip.
    */
  def stoppableSource[T, M](source: Source[T, M]): SourceRef[T, M] = {

    // Note, we cannot just do `takeWhile(_ => !promise.isCompleted)` because it will
    // only take effect if something is emitted via the source. The workaround it to
    // merge with a source on the future there will be an item that will trigger the
    // takeWhile condition.
    val promise = Promise[Done]()
    val stoppable = source
      .merge(Source.fromFuture(promise.future))
      .takeWhile(!_.isInstanceOf[Done])
      .map(_.asInstanceOf[T])
    SourceRef(stoppable, promise)
  }

  /**
    * Source that will repeat the item every `delay`. The first item will get pushed
    * immediately.
    */
  def repeat[T](item: T, delay: FiniteDuration): Source[T, NotUsed] = {
    Source.repeat(item).throttle(1, delay, 1, ThrottleMode.Shaping)
  }

  /**
    * Source that will repeat the item with the specified delay in between while the
    * condition is still true.
    */
  def repeatWhile[T](item: T, delay: FiniteDuration): (Source[T, NotUsed], AtomicBoolean) = {
    val continue = new AtomicBoolean(true)
    val iterator = new Iterator[T] {
      override def hasNext: Boolean = continue.get()
      override def next(): T = item
    }
    val src = Source.fromIterator(() => iterator).throttle(1, delay, 1, ThrottleMode.Shaping)
    src -> continue
  }

  /**
    * Frames an SSE stream by new line. This is to ensure that a message is not broken
    * up in the middle. The LF is used instead of CRLF because some SSE sources are more
    * lax.
    */
  def sseFraming: Flow[ByteString, ByteString, NotUsed] = {
    Framing.delimiter(ByteString("\n"), 65536, allowTruncation = true)
  }

  /**
    * Creates a flow that increments the counter foreach item that comes through. The items
    * themselves are not modified in any way.
    *
    * @param c
    *     Counter to increment.
    * @tparam T
    *     Types of the items flowing through.
    * @return
    *     Flow for counting the number of events flowing through.
    */
  def countEvents[T](c: Counter): Flow[T, T, NotUsed] = {
    Flow[T].map { v =>
      c.increment(); v
    }
  }

}
