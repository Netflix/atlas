/*
 * Copyright 2015 Netflix, Inc.
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

import akka.actor._
import com.netflix.atlas.config.ConfigManager
import com.netflix.spectator.api.Spectator
import spray.can.Http
import spray.can.server.Stats
import spray.http.HttpMethods._
import spray.http._
import spray.routing._


class RequestHandlerActor extends Actor with ActorLogging with HttpService {

  import com.netflix.atlas.akka.CustomDirectives._
  import scala.concurrent.duration._

  private val serverStats: ServerStats = new ServerStats(Spectator.registry())

  def actorRefFactory = context

  def receive: Receive = {
    val endpoints = loadRoutesFromConfig()
    if (endpoints.isEmpty) default else {
      default.orElse {
        runRoute {
          compressResponseIfRequested() {
            corsFilter {
              endpoints.tail.foldLeft(endpoints.head.routes) { case (acc, r) => acc ~ r.routes }
            }
          }
        }
      }
    }
  }

  private val default: Actor.Receive = {
    case _: Http.Bound =>
      val sys = context.system
      sys.scheduler.schedule(0.seconds, 10.seconds, sender(), Http.GetStats)(sys.dispatcher, self)

    case _: Http.Connected => sender() ! Http.Register(self)

    case stats: Stats =>
      serverStats.update(stats)

    // For CORS pre-flight
    case HttpRequest(OPTIONS, _, _, _, _) =>
      sender() ! HttpResponse(status = StatusCodes.OK)

    case HttpRequest(_, Uri.Path("/healthcheck"), _, _, _) =>
      sender() ! HttpResponse(status = StatusCodes.OK)

    case Timedout(HttpRequest(method, uri, _, _, _)) =>
      val errorMsg = DiagnosticMessage.error(s"request timed out: $method $uri")
      val entity = HttpEntity(MediaTypes.`application/json`, errorMsg.toJson)
      sender ! HttpResponse(status = StatusCodes.InternalServerError, entity)
  }

  private def loadRoutesFromConfig(): List[WebApi] = {
    import scala.collection.JavaConversions._
    val config = ConfigManager.current
    val routeClasses = config.getStringList("atlas.akka.api-endpoints").toList
    routeClasses.map { cls =>
      val c = Class.forName(cls)
      val ctor = c.getConstructor(classOf[ActorRefFactory])
      ctor.newInstance(context).asInstanceOf[WebApi]
    }
  }
}
