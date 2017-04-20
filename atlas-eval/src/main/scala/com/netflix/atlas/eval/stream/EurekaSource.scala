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
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object EurekaSource extends StrictLogging {

  type Client = Flow[(HttpRequest, NotUsed), (Try[HttpResponse], NotUsed), NotUsed]

  type InstanceSource = Source[ByteString, NotUsed]
  type InstanceSources = List[InstanceSource]
  type InstanceSourceRef = SourceRef[ByteString, NotUsed]

  /**
    * Subscribes to all instances that are available for an app or a vip in eureka.
    *
    * @param eurekaUri
    *     Should be either the `/v2/apps/{app}` or `/v2/vips/{vip}` endpoint for a
    *     Eureka service. Depending on the Eureka service configuration there may be
    *     some variation in the path.
    * @param instanceUriPattern
    *     Pattern for constructing the URI for each instance in Eureka. The allowed
    *     substitutions are `{port}` and any key in the data center info metadata
    *     block for an instance. Example:
    *
    *     ```
    *     http://{local-ipv4}:{port}/metrics?q=name,cpu,:eq,:sum
    *     ```
    * @param client
    *     HTTP client flow used for getting data from Eureka and for consuming from
    *     the instances.
    */
  def apply(eurekaUri: String, instanceUriPattern: String, client: Client): Source[ByteString, NotUsed] = {
    EvaluationFlows.repeat(NotUsed, 30.seconds)
      .via(fetchEurekaData(eurekaUri, client))
      .via(new InstanceHandler(instanceUriPattern, client))
      .flatMapMerge(Int.MaxValue, sources => Source(sources).flatMapMerge(Int.MaxValue, s => s))
  }

  private def fetchEurekaData(eurekaUri: String, client: Client): Flow[NotUsed, EurekaResponse, NotUsed] = {
    val useVipFormat = eurekaUri.contains("/vips/")
    val headers = List(
      Accept(MediaTypes.`application/json`),
      `Accept-Encoding`(HttpEncodings.gzip))
    val request = HttpRequest(HttpMethods.GET, eurekaUri, headers)

    Flow[NotUsed]
      .map(_ => request -> NotUsed)
      .via(client)
      .flatMapConcat {
        case (Success(res: HttpResponse), _) if res.status == StatusCodes.OK =>
          parseResponse(res, useVipFormat)
        case (Success(res: HttpResponse), _) =>
          logger.warn(s"eureka refresh failed with status ${res.status}")
          Source.empty[EurekaResponse]
        case (Failure(t), _) =>
          logger.warn(s"eureka refresh failed with exception", t)
          Source.empty[EurekaResponse]
      }
  }

  private def unzipIfNeeded(res: HttpResponse): Source[ByteString, Any] = {
    val isCompressed = res.headers.contains(`Content-Encoding`(HttpEncodings.gzip))
    if (isCompressed) res.entity.dataBytes.via(Compression.gunzip()) else res.entity.dataBytes
  }

  private def parseResponse(res: HttpResponse, useVipFormat: Boolean): Source[EurekaResponse, Any] = {
    unzipIfNeeded(res)
      .reduce(_ ++ _)
      .map { bs =>
        if (useVipFormat)
          Json.decode[VipResponse](bs.toArray)
        else
          Json.decode[AppResponse](bs.toArray)
      }
  }

  class InstanceHandler(instanceUriPattern: String, client: Client)
    extends GraphStage[FlowShape[EurekaResponse, InstanceSources]] {

    private val in = Inlet[EurekaResponse]("InstanceHandler.in")
    private val out = Outlet[InstanceSources]("InstanceHandler.out")

    override val shape: FlowShape[EurekaResponse, InstanceSources] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private val instanceMap = new collection.mutable.AnyRefMap[String, InstanceSourceRef]

        private def instanceUri(instance: Instance): String = {
          var uri = instanceUriPattern
          instance.dataCenterInfo.metadata.foreach { case (k, v) =>
            uri = uri.replace(s"{$k}", v)
          }
          uri = uri.replace("{port}", instance.port.toString)
          uri
        }

        override def onPush(): Unit = {
          val instances = grab(in).instances
          val currentIds = instanceMap.keySet
          val foundInstances = instances.map(i => i.instanceId -> i).toMap

          val added = foundInstances -- currentIds
          logger.info(s"instances added: ${mkString(added)}")
          val sources = added.values.map { instance =>
            val uri = instanceUri(instance)
            val ref = EvaluationFlows.stoppableSource(HostSource(uri, client))
            instanceMap += instance.instanceId -> ref
            ref.source
          }
          push(out, sources.toList)

          val removed = instanceMap.toMap -- foundInstances.keySet
          logger.info(s"instances removed: ${mkString(removed)}")
          removed.foreach { case (id, ref) =>
            instanceMap -= id
            ref.stop()
          }
        }

        private def mkString(m: Map[String, _]): String = {
          m.keySet.toList.sorted.mkString(",")
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

  sealed trait EurekaResponse {
    def instances: List[Instance]
  }

  case class VipResponse(applications: Apps) extends EurekaResponse {
    def instances: List[Instance] = applications.application.flatMap(_.instance)
  }

  case class AppResponse(application: App) extends EurekaResponse {
    def instances: List[Instance] = application.instance
  }

  case class Apps(application: List[App])

  case class App(name: String, instance: List[Instance])

  case class Instance(
    instanceId: String,
    status: String,
    dataCenterInfo: DataCenterInfo,
    port: PortInfo
  )

  case class DataCenterInfo(name: String, metadata: Map[String, String])

  case class PortInfo(`$`: Int) {
    def port: Int = `$`
    override def toString: String = `$`.toString
  }
}
