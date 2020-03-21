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
package com.netflix.atlas.akka

import java.util.concurrent.ArrayBlockingQueue

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.RestartFlow
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.akka.StreamOps.QueueSource
import com.netflix.atlas.akka.StreamOps.SourceQueue
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
  * Utility functions for commonly used operations to work with a cluster of instances.
  */
object ClusterOps extends StrictLogging {

  private val noopRegistry = new NoopRegistry

  /**
    * Maintains a sub-flow for each member of a cluster. The set of sub-flows can change
    * dynamically based on the members present in the input cluster definition.
    *
    * ```
    * [C(n)|D(n)] ---> [D1] -> client(M1) -> [O1] --+---> [O]
    *             |                                 |
    *             |--> [D2] -> client(M2) -> [O2] --|
    *             |                                 |
    *             :              ...                :
    *             +--> [Dn] -> client(Mn) -> [On] --+
    * ```
    *
    * When a [Cluster] message is received, it will sync the set of member sub-flows with
    * the set of members in the cluster definition.
    *
    * A queue will be maintained for each member of the cluster. When a [Data] message is
    * received, the data items will be passed to the queue for the corresponding member. If
    * the queue fills up, then data for that member will get dropped.
    *
    * @param context
    *     Parameters to control the behavior of the operation.
    * @tparam M
    *     Key identifying a member of a cluster. Should have a clean toString value that is
    *     appropriate for logging.
    * @tparam D
    *     Input data to forward to the sub-flow for a member.
    * @tparam O
    *     Output data that will be flattened after the mapping.
    * @return
    *     Overall flow that performs the grouping and merges the output data from the
    *     cluster.
    */
  def groupBy[M <: AnyRef, D, G <: GroupByMessage[M, D], O](
    context: GroupByContext[M, D, O]
  ): Flow[G, O, NotUsed] = {
    Flow[G]
      .via(new ClusterGroupBy[M, D, G, O](context))
      .flatMapMerge(Int.MaxValue, sources => Source(sources))
      .flatMapMerge(Int.MaxValue, source => source)
  }

  private final class ClusterGroupBy[M <: AnyRef, D, G <: GroupByMessage[M, D], O](
    context: GroupByContext[M, D, O]
  ) extends GraphStage[FlowShape[G, List[Source[O, NotUsed]]]] {

    private val in = Inlet[GroupByMessage[M, D]]("ClusterGroupBy.in")
    private val out = Outlet[List[Source[O, NotUsed]]]("ClusterGroupBy.out")

    override def shape: FlowShape[GroupByMessage[M, D], List[Source[O, NotUsed]]] =
      FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with InHandler with OutHandler {

        private val registry = new NoopRegistry
        private val membersSources = mutable.AnyRefMap.empty[M, SourceQueue[D]]

        override def onPush(): Unit = {
          val msg = grab(in)
          msg match {
            case Cluster(members) => updateMembers(members)
            case Data(data)       => pushData(data)
          }
        }

        private def updateMembers(members: Set[M]): Unit = {
          val current = membersSources.keySet

          val removed = current -- members
          if (removed.nonEmpty) {
            logger.debug(s"members removed: $removed")
          }
          removed.foreach { m =>
            membersSources.remove(m).foreach { queue =>
              logger.debug(s"stopping $m")
              queue.complete()
            }
          }

          val added = members -- current
          if (added.nonEmpty) {
            logger.debug(s"members added: $added")
          }
          val sources = added.toList
            .map { m =>
              val queue =
                new SourceQueue[D](
                  registry,
                  "ClusterGroupBy",
                  new ArrayBlockingQueue[D](context.queueSize)
                )
              membersSources += m -> queue
              Source
                .fromGraph(new QueueSource[D](() => queue))
                .mapMaterializedValue(_ => NotUsed)
                .via(newSubFlow(m))
            }

          push(out, sources)
        }

        private def newSubFlow(m: M): Flow[D, O, NotUsed] = {
          implicit val xc: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
          RestartFlow.withBackoff(100.millis, 1.second, 0.0) { () =>
            context.client(m).watchTermination() { (_, f) =>
              f.onComplete {
                case Success(_) => logger.trace(s"shutdown stream for $m")
                case Failure(t) => logger.warn(s"restarting failed stream for $m", t)
              }
            }
          }
        }

        private def pushData(data: Map[M, D]): Unit = {
          data.foreachEntry { (m, d) =>
            membersSources.get(m).foreach(_.offer(d))
          }
          if (isAvailable(out)) {
            pull(in)
          }
        }

        override def onPull(): Unit = {
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          membersSources.values.foreach(_.complete())
          super.onUpstreamFinish()
        }

        setHandlers(in, out, this)
      }
    }
  }

  /**
    * Context settings for the cluster group by operation.
    *
    * @param client
    *     Function that creates a sub-flow for a member of a cluster. The input data for
    *     the member will be emitted to the sub-flow. If a failure occurs for the client
    *     flow, then it will be recreated continually until the member is removed from the
    *     cluster definition.
    * @param registry
    *     Registry providing basic stats for the queue such as whether or not data is being
    *     dropped. Default is NoopRegistry.
    * @param id
    *     Id used with the queue metrics.
    * @param queueSize
    *     Size of the queue for each sub-stream. If the queue fills up, then new incoming data
    *     will be dropped. Default size is 1.
    * @tparam M
    *     Key identifying a member of a cluster. Should have a clean toString value that is
    *     appropriate for logging.
    * @tparam D
    *     Input data to forward to the sub-flow for a member.
    * @tparam O
    *     Output data that will be flattened after the mapping.
    */
  case class GroupByContext[M <: AnyRef, D, O](
    client: M => Flow[D, O, NotUsed],
    registry: Registry = noopRegistry,
    id: String = "clusterGroupBy",
    queueSize: Int = 1
  )

  /** Base type for messages to a cluster operation. */
  sealed trait GroupByMessage[+M <: AnyRef, +D]

  /** Defines the set of members for a cluster. It should always be the complete set. */
  case class Cluster[M <: AnyRef, D](members: Set[M]) extends GroupByMessage[M, D]

  /** Data intended for members of a cluster. */
  case class Data[M <: AnyRef, D](data: Map[M, D]) extends GroupByMessage[M, D]
}
