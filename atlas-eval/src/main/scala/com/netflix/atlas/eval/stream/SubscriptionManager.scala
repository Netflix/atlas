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

import java.util.Collections

import akka.NotUsed
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.Uri
import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.OverflowStrategy
import akka.stream.SinkShape
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.InHandler
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success

/**
  * Manages a set of streams based on the incoming data sources.
  */
private[stream] class SubscriptionManager(context: StreamContext)
    extends GraphStageWithMaterializedValue[SinkShape[SourcesAndGroups], Future[NotUsed]]
    with StrictLogging {

  import EurekaSource._
  import Evaluator._
  import StreamContext._
  import SubscriptionManager._

  import scala.concurrent.ExecutionContext.Implicits.global

  private val in = Inlet[SourcesAndGroups]("SubscriptionManager.in")

  override val shape: SinkShape[SourcesAndGroups] = SinkShape(in)

  def createLogicAndMaterializedValue(attrs: Attributes): (GraphStageLogic, Future[NotUsed]) = {
    val promise = Promise[NotUsed]()
    createLogic(attrs, promise) -> promise.future
  }

  private def createLogic(attrs: Attributes, promise: Promise[NotUsed]): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler {

      private var sources = new DataSources(Collections.emptySet())

      // EurekaUri -> (InstanceUriPattern, SubscribePayloadJson)
      private var backendDataSources = Map.empty[String, (String, String)]

      // Needs to be lazy because the materializer will not be usable until the
      // stage is fully initialized
      private lazy val http = Source
        .queue[HttpRequest](2048, OverflowStrategy.dropHead)
        .via(context.httpClient("subscribe"))
        .to(Sink.foreach { response =>
          response.foreach(_.discardEntityBytes()(materializer))
        })
        .run()(materializer)

      private def logSources(change: String, sources: List[DataSource]): Unit = {
        val n = sources.size
        logger.info(s"$change $n sources")
        sources.zipWithIndex.foreach {
          case (source, i) => logger.debug(s"$i of $n, $change: $source")
        }
      }

      private def handleDataSources(dataSources: DataSources): Unit = {
        // Log changes to the data sources for easier debugging
        import scala.collection.JavaConverters._
        logSources("added", dataSources.addedSources(sources).asScala.toList)
        logSources("removed", dataSources.removedSources(sources).asScala.toList)
        sources = dataSources

        // Generate the subscribe payloads to use for each eureka backend. Other backend
        // types, e.g. file, have predefined data sets and thus this sink does not need
        // to do anything for them.
        backendDataSources = dataSources.getSources.asScala
          .map { ds =>
            context.findBackendForUri(Uri(ds.getUri)) -> ds
          }
          .collect {
            case (b: EurekaBackend, ds) => b -> ds
          }
          .groupBy(_._1)
          .map {
            case (b, ds) =>
              val payload = b.instanceUri -> toSubscribeRequest(ds.map(_._2).toList)
              b.eurekaUri -> payload
          }
      }

      private def toSubscribeRequest(ds: List[DataSource]): String = {
        val exprs = ds.flatMap { d =>
          context.interpreter.eval(Uri(d.getUri)).map { expr =>
            Expression(expr.toString, d.getStep.toMillis)
          }
        }
        Json.encode(SubscribePayload(context.id, exprs))
      }

      private def mkRequest(uri: String, payload: String): HttpRequest = {
        val entity = HttpEntity(MediaTypes.`application/json`, payload)
        HttpRequest(HttpMethods.POST, Uri(uri), entity = entity)
      }

      private def handleGroups(groups: Groups): Unit = {
        groups.groups.foreach { group =>
          backendDataSources.get(group.uri) match {
            case Some((uriPattern, payload)) =>
              group.instances.foreach { instance =>
                val uri = instance.substitute(uriPattern) + "/lwc/api/v1/subscribe"
                http.offer(mkRequest(uri, payload))
              }
            case None =>
              logger.debug(s"no data sources for group: ${group.uri}")
          }
        }
      }

      override def preStart(): Unit = {
        pull(in)
      }

      override def onPush(): Unit = {
        val (ds, groups) = grab(in)
        handleDataSources(ds)
        handleGroups(groups)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
        http.watchCompletion().onComplete(_ => promise.complete(Success(NotUsed)))
        http.complete()
      }

      setHandler(in, this)
    }
  }
}

object SubscriptionManager {
  case class SubscribePayload(streamId: String, expressions: List[Expression])

  case class Expression(expression: String, frequency: Long = 60000L)
}
