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
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Spectator
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import spray.can.Http
import spray.can.server.Stats
import spray.http.HttpMethods._
import spray.http._
import spray.routing._


class RequestHandlerActor(registry: Registry, config: Config)
    extends Actor with StrictLogging with HttpService {

  def this() = this(Spectator.globalRegistry(), ConfigManager.current)

  import com.netflix.atlas.akka.CustomDirectives._
  import scala.concurrent.duration._

  private val serverStats: ServerStats = new ServerStats(registry)

  def actorRefFactory = context

  def receive: Receive = {
    val endpoints = loadRoutesFromConfig()
    if (endpoints.isEmpty) default else {
      default.orElse {
        runRoute {
          accessLog {
            compressResponseIfRequested() {
              decompressRequest() {
                corsFilter {
                  endpoints.tail.foldLeft(endpoints.head.routes) { case (acc, r) => acc ~ r.routes}
                }
              }
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

  /**
   * In many cases the final list will come from several config files with values getting appended
   * to the list. To avoid unnecessary duplication the class list will be deduped so that only
   * the first instance of a class will be used. The order in the list is otherwise maintained.
   */
  private def loadRoutesFromConfig(): List[WebApi] = {
    import scala.collection.JavaConversions._
    val routeClasses = config.getStringList("atlas.akka.api-endpoints").toList.distinct
    routeClasses.map { cls =>
      logger.info(s"loading webapi class: $cls")
      val c = Class.forName(cls)
      val ctor = c.getConstructor(classOf[ActorRefFactory])
      ctor.newInstance(context).asInstanceOf[WebApi]
    }
  }
}
