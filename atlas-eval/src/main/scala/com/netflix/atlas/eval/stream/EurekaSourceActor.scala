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

import java.util.UUID

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

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
  * @param sink
  *     Sink to use for all data coming from instances. The response streams from
  *     each instance will get framed by new line and then fed into the sink.
  */
class EurekaSourceActor(
  eurekaUri: String,
  instanceUriPattern: String,
  sink: Sink[ByteString, _]) extends Actor with StrictLogging {

  import EurekaSourceActor._

  import context.dispatcher

  private implicit val system = context.system
  private implicit val materializer = ActorMaterializer()

  private val useVipFormat = eurekaUri.contains("/vips/")

  private val ticker = system.scheduler.schedule(0.seconds, 5.seconds, self, Tick)

  private val instanceMap = new collection.mutable.AnyRefMap[String, StreamRef[_]]

  def receive: Receive = {
    case Tick                      => fetchEurekaData()
    case Success(res: VipResponse) => updateInstanceMap(res.instances)
    case Success(res: AppResponse) => updateInstanceMap(res.instances)
    case Failure(t)                => logger.warn("failed to refresh eureka data", t)
  }

  private def updateInstanceMap(instances: List[Instance]): Unit = {
    val currentIds = instanceMap.keySet
    val foundInstances = instances.map(i => i.instanceId -> i).toMap
    handleAddedInstances(foundInstances -- currentIds)
    handleRemovedInstances(instanceMap.toMap -- foundInstances.keySet)
  }

  private def handleAddedInstances(instances: Map[String, Instance]): Unit = {
    if (instances.keySet.nonEmpty)
      logger.info(s"added instances: ${instances.keySet.mkString(", ")}")
    instances.values.foreach(subscribe)
  }

  private def handleRemovedInstances(instances: Map[String, StreamRef[_]]): Unit = {
    if (instances.keySet.nonEmpty)
      logger.info(s"removed instances: ${instances.keySet.mkString(", ")}")
    instanceMap --= instances.keySet
    instances.values.foreach(_.killSwitch.shutdown())
  }

  private def instanceUri(instance: Instance): String = {
    var uri = instanceUriPattern
    instance.dataCenterInfo.metadata.foreach { case (k, v) =>
      uri = uri.replace(s"{$k}", v)
    }
    uri = uri.replace("{port}", instance.port.toString)
    uri
  }

  private def subscribe(instance: Instance): Unit = {
    val uri = instanceUri(instance)
    instanceMap += instance.instanceId -> Evaluator.runForHost(uri, sink)
  }

  private def fetchEurekaData(): Unit = {
    val headers = List(
      Accept(MediaTypes.`application/json`),
      `Accept-Encoding`(HttpEncodings.gzip))
    val request = HttpRequest(HttpMethods.GET, eurekaUri, headers)
    Http().singleRequest(request)
      .flatMap { res =>
        if (res.status != StatusCodes.OK) {
          throw new IllegalStateException(s"request failed: ${res.status}")
        }
        val isCompressed = res.headers.contains(`Content-Encoding`(HttpEncodings.gzip))
        val source = if (isCompressed) res.entity.dataBytes.via(Compression.gunzip()) else res.entity.dataBytes
        source
          .runReduce(_ ++ _)
          .map { bs =>
            if (useVipFormat)
              Json.decode[VipResponse](bs.toArray)
            else
              Json.decode[AppResponse](bs.toArray)
          }
      }
      .onComplete(r => self ! r)
  }

  override def postStop(): Unit = {
    ticker.cancel()
    instanceMap.values.foreach(_.killSwitch.shutdown())
    super.postStop()
  }
}

object EurekaSourceActor {

  case object Tick

  case class VipResponse(applications: Apps) {
    def instances: List[Instance] = applications.application.flatMap(_.instance)
  }

  case class AppResponse(application: App) {
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
