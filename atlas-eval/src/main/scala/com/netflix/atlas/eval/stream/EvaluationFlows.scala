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

import java.nio.charset.StandardCharsets

import akka.Done
import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.KillSwitches
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.ServoMessage
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.util.ByteStringInputStream
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Counter

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration


/**
  * Helpers for evaluating Atlas expressions over streaming data sources.
  */
object EvaluationFlows {

  private val servoPrefix = ByteString("data: ")

  /**
    * Run a stream connecting the source to the sink.
    */
  def run[T, M1, M2](source: Source[T, M1], sink: Sink[T, M2])
    (implicit materializer: ActorMaterializer): StreamRef[M2] = {

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
    Flow[T].map { v => c.increment(); v }
  }

  /**
    * Creates a flow that maps an SSE stream from the LWC service into a stream of
    * [[AggrDatapoint]] objects.
    */
  def lwcToAggrDatapoint: Flow[String, AggrDatapoint, NotUsed] = {
    Flow[String].via(new LwcToAggrDatapoint)
  }

  /**
    * Creates a flow that converts a servo SSE stream into a stream of [[Datapoint]]
    * objects.
    *
    * @param step
    *     Step size used for the input source. To ensure accurate results this should
    *     match the step size actually used on the source.
    * @return
    *     Stream of datapoints.
    */
  def servoMessagesToDatapoints(step: Long): Flow[ByteString, Datapoint, NotUsed] = {
    Flow[ByteString]
      .filter(_.startsWith(servoPrefix))
      .map { data =>
        val slice = data.slice(servoPrefix.size, data.size)
        Json.decode[ServoMessage](new ByteStringInputStream(slice))
      }
      .flatMapConcat(msg => Source(msg.metrics.map(_.toDatapoint(step))))
  }

  /**
    * Creates a flow that will perform an online evaluation of raw datapoints.
    *
    * @param expr
    *     User expression to evaluate. Note, that the online evaluation does not support
    *     time shifts. TODO, what behavior should we have for time shifts?
    * @param step
    *     Step size used for the input source. To ensure accurate results this should
    *     match the step size actually used on the source.
    * @return
    *     Time series messages containing the results of the evaluation.
    */
  def forDatapoints(expr: StyleExpr, step: Long): Flow[Datapoint, TimeSeriesMessage, NotUsed] = {
    Flow[Datapoint]
      .via(new TimeGrouped[Datapoint](2, 50, _.timestamp))
      .via(new DatapointEval[Datapoint](expr, step))
      .flatMapConcat(msgs => Source(msgs))
  }

  /**
    * Creates a flow that will perform an online evaluation of partially aggregated
    * datapoints.
    *
    * @param expr
    *     User expression to evaluate. Note, that the online evaluation does not support
    *     time shifts. TODO, what behavior should we have for time shifts?
    * @param step
    *     Step size used for the input source. To ensure accurate results this should
    *     match the step size actually used on the source.
    * @return
    *     Time series messages containing the results of the evaluation.
    */
  def forPartialAggregates(expr: StyleExpr, step: Long): Flow[AggrDatapoint, JsonSupport, NotUsed] = {
    // TODO: number of buffers should be configurable by user, need to discuss how best to
    // map some that into the api... for now it is set to 1 to reduce the delay during testing
    Flow[AggrDatapoint]
      .via(new TimeGrouped[AggrDatapoint](1, 50, _.timestamp))
      .via(new DataExprEval(expr, step))
      .flatMapConcat(msgs => Source(msgs))
  }

  def lwcEval(expr: StyleExpr, step: Long): Flow[ByteString, JsonSupport, NotUsed] = {
    Flow[ByteString]
      .map(_.decodeString(StandardCharsets.UTF_8))
      .via(EvaluationFlows.lwcToAggrDatapoint)
      .via(EvaluationFlows.forPartialAggregates(expr, step))
  }
}
