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
package com.netflix.atlas.pekko

import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Props
import org.apache.pekko.routing.FromConfig
import com.netflix.iep.service.AbstractService
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.lang.reflect.Type

/**
  * Exposes actor system as service for healthcheck and proper shutdown. Additional
  * actors to start up can be specified using the `atlas.pekko.actors` property.
  */
class ActorService(
  system: ActorSystem,
  config: Config,
  registry: Registry,
  classFactory: ClassFactory
) extends AbstractService
    with StrictLogging {

  override def startImpl(): Unit = {
    import scala.jdk.CollectionConverters.*
    config.getConfigList("atlas.pekko.actors").asScala.foreach { cfg =>
      val name = cfg.getString("name")
      val cls = Class.forName(cfg.getString("class"))
      val ref = system.actorOf(newActor(name, cls), name)
      logger.info(s"created actor '${ref.path}' using class '${cls.getName}'")
    }
  }

  private def newActor(name: String, cls: Class[?]): Props = {
    import scala.compat.java8.FunctionConverters.*
    val bindings = Map[Type, AnyRef](
      classOf[Config]   -> config,
      classOf[Registry] -> registry
    ).withDefaultValue(null)
    // Force eager loading of dependencies to avoid locking issues with singleton beans
    classFactory.verifyDependencies(cls, bindings.asJava)
    val props = Props(classFactory.newInstance[Actor](cls, bindings.asJava))
    val routerCfgPath = s"pekko.actor.deployment./$name.router"
    if (config.hasPath(routerCfgPath)) FromConfig.props(props) else props
  }

  override def stopImpl(): Unit = {}
}
