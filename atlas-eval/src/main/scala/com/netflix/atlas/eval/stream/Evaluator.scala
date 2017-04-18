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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.Props
import akka.stream.ActorMaterializer
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergeHub
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.ServoMessage
import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Counter


/**
  * Helpers for evaluating Atlas expressions over streaming data sources.
  */
object Evaluator {

  // Jackson cannot read from the [[ByteString]] directly so we need to create or copy
  // into an array. This keeps track of arrays per thread to avoid the need to allocate
  // a new byte array each time.
  private val buffers = ThreadLocal.withInitial[Array[Byte]](() => new Array[Byte](8192))

  private val servoPrefix = ByteString("data: ")

  /**
    * Run a stream that collects data from `uri` and feeds it to the provided sink. The
    * stream will not stop on its own. Use the kill switch in the provided stream ref to
    * shutdown when the data is no longer of interest.
    */
  def runForHost[T](uri: String, sink: Sink[ByteString, T])
    (implicit system: ActorSystem, materializer: ActorMaterializer): StreamRef[T] = {

    val (killSwitch, value) = HostSource(uri)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(sink)(Keep.both)
      .run()

    StreamRef(killSwitch, value)
  }

  /**
    * Run a stream that collects data from all instances registered in Eureka for a give
    * application or vip and feed it to the provided sink. The stream will not stop on its
    * own. Use the kill switch in the provided stream ref to shutdown when the data is no
    * longer of interest.
    */
  def runForEurekaVip[T](eurekaUri: String, instanceUri: String, sink: Sink[ByteString, T])
    (implicit system: ActorSystem, materializer: ActorMaterializer): StreamRef[T] = {

    val runnableGraph = MergeHub
      .source[ByteString](perProducerBufferSize = 16)
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(sink)(Keep.both)
    val ((toConsumer, killSwitch), value) = runnableGraph.run()

    val actorRef = system.actorOf(Props(new EurekaSourceActor(eurekaUri, instanceUri, toConsumer)))
    val switch = new KillSwitch {
      override def abort(ex: Throwable): Unit = shutdown()
      override def shutdown(): Unit = {
        system.stop(actorRef)
        killSwitch.shutdown()
      }
    }
    StreamRef(switch, value)
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
        val buffer = buffers.get()
        if (slice.size < buffer.length) {
          slice.copyToArray(buffer)
          Json.decode[ServoMessage](buffer, 0, slice.size)
        } else {
          val buf = slice.toArray
          buffers.set(buf)
          Json.decode[ServoMessage](buf)
        }
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
  def forPartialAggregates(expr: StyleExpr, step: Long): Flow[AggrDatapoint, TimeSeriesMessage, NotUsed] = {
    Flow[AggrDatapoint]
      .via(new TimeGrouped[AggrDatapoint](2, 50, _.timestamp))
      .via(new DataExprEval(expr, step))
      .flatMapConcat(msgs => Source(msgs))
  }
}
