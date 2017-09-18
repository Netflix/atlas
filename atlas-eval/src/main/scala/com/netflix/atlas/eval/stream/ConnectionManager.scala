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

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.eval.stream.EurekaSource.Groups
import com.typesafe.scalalogging.StrictLogging

/**
  * Manages a set of connections to all instances for a set of Eureka groups and collects
  * the data from the `/lwc/api/v1/stream/$id` endpoint.
  */
private[stream] class ConnectionManager(context: StreamContext)
    extends GraphStage[FlowShape[Groups, InstanceSources]]
    with StrictLogging {

  private val in = Inlet[Groups]("ConnectionManager.in")
  private val out = Outlet[InstanceSources]("ConnectionManager.out")

  override val shape: FlowShape[Groups, InstanceSources] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val instanceMap = scala.collection.mutable.AnyRefMap.empty[String, InstanceSourceRef]

      private def mkString(m: Map[String, _]): String = {
        m.keySet.toList.sorted.mkString(",")
      }

      override def onPush(): Unit = {
        val instances = grab(in).groups.flatMap(_.instances)
        val currentIds = instanceMap.keySet
        val foundInstances = instances.map(i => i.instanceId -> i).toMap

        val added = foundInstances -- currentIds
        if (added.nonEmpty) {
          logger.debug(s"instances added: ${mkString(added)}")
        }

        val path = "/lwc/api/v1/stream/" + context.id
        val sources = added.values.map { instance =>
          val uri = instance.substitute("http://{local-ipv4}:{port}") + path
          val ref = EvaluationFlows.stoppableSource(HostSource(uri, context.httpClient("stream")))
          instanceMap += instance.instanceId -> ref
          ref.source
        }
        push(out, sources.toList)

        val removed = instanceMap.toMap -- foundInstances.keySet
        if (removed.nonEmpty) {
          logger.debug(s"instances removed: ${mkString(removed)}")
        }
        removed.foreach {
          case (id, ref) =>
            instanceMap -= id
            ref.stop()
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

      setHandlers(in, out, this)
    }
  }
}
