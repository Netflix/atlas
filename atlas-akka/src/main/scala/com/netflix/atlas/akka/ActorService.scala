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
package com.netflix.atlas.akka

import javax.inject.Inject
import javax.inject.Singleton

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.routing.FromConfig
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Exposes actor system as service for healthcheck and proper shutdown. Additional
  * actors to start up can be specified using the `atlas.akka.actors` property.
  */
@Singleton
class ActorService @Inject() (system: ActorSystem, config: Config, classFactory: ClassFactory)
  extends AbstractService with StrictLogging {

  override def startImpl(): Unit = {
    import scala.collection.JavaConverters._
    config.getConfigList("atlas.akka.actors").asScala.foreach { cfg =>
      val name = cfg.getString("name")
      val cls = Class.forName(cfg.getString("class"))
      val ref = system.actorOf(newActor(name, cls), name)
      logger.info(s"created actor '${ref.path}' using class '${cls.getName}'")
    }
  }

  private def newActor(name: String, cls: Class[_]): Props = {
    val props = Props(classFactory.newInstance[Actor](cls))
    val routerCfgPath = s"akka.actor.deployment./$name.router"
    if (config.hasPath(routerCfgPath)) FromConfig.props(props) else props
  }

  override def stopImpl(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
  }
}
