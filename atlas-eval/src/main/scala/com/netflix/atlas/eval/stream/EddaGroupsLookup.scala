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
package com.netflix.atlas.eval.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import com.netflix.atlas.eval.stream.EddaSource.GroupResponse
import com.netflix.atlas.eval.stream.EddaSource.Groups
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.FiniteDuration

/**
  * Takes in a collection of data sources and outputs a source that will regularly emit
  * the metadata for the groups needed by those sources.
  */
private[stream] class EddaGroupsLookup(context: StreamContext, frequency: FiniteDuration)
    extends GraphStage[FlowShape[DataSources, Source[SourcesAndGroups, NotUsed]]]
    with StrictLogging {

  private val in = Inlet[DataSources]("EurekaGroupsLookup.in")
  private val out = Outlet[Source[SourcesAndGroups, NotUsed]]("EurekaGroupsLookup.out")

  override val shape: FlowShape[DataSources, Source[SourcesAndGroups, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var lookupTickSwitch: Option[SourceRef[NotUsed, NotUsed]] = None

      override def onPush(): Unit = {
        import scala.jdk.CollectionConverters.*

        // If there is an existing source polling Eureka, then tell it to stop. Create a
        // new instance of the flag for the next source
        lookupTickSwitch.foreach(_.stop())

        val next = grab(in)

        // Create a list of sources, one for each distinct Eureka group that is needed
        // by one of the data sources
        if (next.sources.isEmpty) {
          // If the Eureka based sources are empty, then just use an empty source to avoid
          // potential delays to shutting down when the upstream completes.
          lookupTickSwitch = None // No need to stop Source.single
          push(out, Source.single[SourcesAndGroups](DataSources.empty() -> Groups(List.empty)))
        } else {
          val eurekaSources = next.sources.asScala
            .flatMap { s =>
              try {
                Option(context.findEurekaBackendForUri(Uri(s.uri)).eddaUri)
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
              EddaSource(uri, context)
            }

          // Perform lookup for each vip and create groups composite
          val lookup = Source(eurekaSources)
            .flatMapConcat(s => s)
            .fold(List.empty[GroupResponse])((acc, g) => g :: acc)
            .map(gs => next -> Groups(gs))

          // Regularly refresh the metadata until it is stopped
          val lookupTickSourceRef = EvaluationFlows.stoppableSource[NotUsed, NotUsed](
            EvaluationFlows.repeat(NotUsed, frequency)
          )
          lookupTickSwitch = Option(lookupTickSourceRef)
          push(out, lookupTickSourceRef.source.flatMapConcat(_ => lookup))
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
        lookupTickSwitch.foreach(_.stop())
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.onUpstreamFailure(ex)
        lookupTickSwitch.foreach(_.stop())
      }

      setHandlers(in, out, this)
    }
  }
}
