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

import java.util.Collections

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.json.JsonSupport
import com.typesafe.scalalogging.StrictLogging

/**
  * Manages a set of streams based on the incoming data sources.
  *
  * @param newEvalSource
  *     Factory method for creating a new evaluation stream based on a data source.
  */
class DataSourceManager(newEvalSource: Evaluator.DataSource => Source[JsonSupport, NotUsed])
    extends GraphStage[
      FlowShape[Evaluator.DataSources, Source[Evaluator.MessageEnvelope, NotUsed]]
    ]
    with StrictLogging {

  import Evaluator._

  private val in = Inlet[DataSources]("DataSourceManager.in")
  private val out = Outlet[Source[MessageEnvelope, NotUsed]]("DataSourceManager.out")

  override val shape: FlowShape[DataSources, Source[MessageEnvelope, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val sources =
        new scala.collection.mutable.AnyRefMap[DataSource, SourceRef[MessageEnvelope, NotUsed]]()
      private var current = new Evaluator.DataSources(Collections.emptySet())

      override def onPush(): Unit = {
        import scala.collection.JavaConverters._
        val next = grab(in)

        val added = next.addedSources(current).asScala.toList
        logger.info(s"added: ${added.mkString(", ")}")
        val addedSources = added.map { ds =>
          val source = newEvalSource(ds).map(t => new MessageEnvelope(ds.getId, t))
          val ref = EvaluationFlows.stoppableSource(source)
          ds -> ref
        }
        sources ++= addedSources
        push(out, Source(addedSources.map(_._2.source)).flatMapMerge(Int.MaxValue, v => v))

        val removed = next.removedSources(current).asScala.toList
        logger.info(s"removed: ${removed.mkString(", ")}")
        removed.foreach { ds =>
          sources.get(ds).foreach(_.stop())
        }

        current = next
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
