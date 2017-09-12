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
import akka.http.scaladsl.model.Uri
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.eval.stream.EurekaSource.GroupResponse
import com.netflix.atlas.eval.stream.EurekaSource.Groups
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration

/**
  * Takes in a collection of data sources and outputs a source that will regularly emit
  * the metadata for the groups needed by those sources.
  */
private[stream] class EurekaGroupsLookup(context: StreamContext, frequency: FiniteDuration)
    extends GraphStage[FlowShape[DataSources, Source[SourcesAndGroups, NotUsed]]]
    with StrictLogging {

  private val in = Inlet[DataSources]("EurekaGroupsLookup.in")
  private val out = Outlet[Source[SourcesAndGroups, NotUsed]]("EurekaGroupsLookup.out")

  override val shape: FlowShape[DataSources, Source[SourcesAndGroups, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var sourceRef: SourceRef[SourcesAndGroups, NotUsed] = _

      override def onPush(): Unit = {
        import scala.collection.JavaConverters._

        // If there is an existing source polling Eureka, then clean it up
        if (sourceRef != null) sourceRef.stop()

        val next = grab(in)

        // Create a list of sources, one for each distinct Eureka group that is needed
        // by one of the data sources
        val eurekaSources = next.getSources.asScala
          .flatMap { s =>
            try {
              Option(context.findEurekaBackendForUri(Uri(s.getUri)).eurekaUri)
            } catch {
              case e: Exception =>
                val msg = DiagnosticMessage.error(e)
                context.dsLogger(s, msg)
                None
            }
          }
          .toList
          .distinct
          .map { uri =>
            EurekaSource(uri, context.client)
          }

        // Perform lookup for each vip and create groups composite
        val lookup = Source(eurekaSources)
          .flatMapConcat(s => s)
          .fold(List.empty[GroupResponse])((acc, g) => g :: acc)
          .map(gs => next -> Groups(gs))

        // Regularly refresh the metadata until the source is stopped
        val src = EvaluationFlows
          .repeat(NotUsed, frequency)
          .flatMapConcat(_ => lookup)
        sourceRef = EvaluationFlows.stoppableSource(src)

        push(out, sourceRef.source)
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
        if (sourceRef != null) sourceRef.stop()
      }

      setHandlers(in, out, this)
    }
  }
}
