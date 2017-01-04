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
package com.netflix.atlas.poller

import java.lang.reflect.Type
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import akka.actor.Actor
import akka.actor.ActorInitializationException
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

/**
  * Manager for a set of poller actors. The manager will send a Tick message to the pollers
  * at a regular interval. The pollers should send by a MetricsPayload with the data
  * collected.
  */
class PollerManager(registry: Registry, classFactory: ClassFactory, config: Config)
  extends Actor with StrictLogging {

  import scala.concurrent.duration._
  private implicit val xc = scala.concurrent.ExecutionContext.global

  private val pollers = PollerManager.pollers(config)
  private val lastUpdateTimes = createPollerActors()

  private val sinkRef = createSinkActor()

  override val supervisorStrategy = OneForOneStrategy() {
    case e: ActorInitializationException =>
      SupervisorStrategy.Escalate
    case e: Exception =>
      logRestart(sender().path, e)
      SupervisorStrategy.Restart
  }

  val frequency = FiniteDuration(
    config.getDuration("atlas.poller.frequency", TimeUnit.NANOSECONDS),
    TimeUnit.NANOSECONDS)
  context.system.scheduler.schedule(frequency, frequency, self, Messages.Tick)

  def receive: Receive = {
    case Messages.Tick =>
      pollers.foreach { p => context.actorSelection(p.name) ! Messages.Tick }
    case p @ Messages.MetricsPayload(_, ds) =>
      val name = sender().path.name
      registry.counter("atlas.poller.datapoints", "id", name).increment(ds.size)
      lastUpdateTimes.get(name).foreach(_.set(registry.clock().wallTime()))
      sinkRef ! p
  }

  private def createPollerActors(): Map[String, AtomicLong] = {
    val updateTimes = pollers.map { p =>
      context.actorOf(Props(classFactory.newInstance[Actor](p.cls)), p.name)
      val updateTime = new AtomicLong(registry.clock().wallTime())
      val id = registry.createId("atlas.poller.dataAge", "id", p.name)
      p.name -> registry.gauge(id, updateTime, Functions.age(registry.clock()))
    }
    updateTimes.toMap
  }

  private def createSinkActor(): ActorRef = {
    import scala.compat.java8.FunctionConverters._
    val cfg = config.getConfig("atlas.poller.sink")
    val cls = cfg.getString("class")
    val overrides = Map[Type, AnyRef](classOf[Config] -> cfg).withDefaultValue(null)
    context.actorOf(
      Props(classFactory.newInstance[Actor](cls, overrides.asJava)),
      PollerManager.SinkName)
  }

  private def logRestart(path: ActorPath, e: Exception): Unit = {
    logger.warn(s"restarting ${path.name}", e)
    val error = e.getClass.getSimpleName
    registry.counter("atlas.poller.restarts", "id", path.name, "error", error).increment()
  }
}

object PollerManager {

  private val SinkName = "sink"

  private def pollers(config: Config): List[PollerConfig] = {
    val builder = List.newBuilder[PollerConfig]
    config.getConfigList("atlas.poller.pollers").forEach { cfg =>
      builder += PollerConfig(s"poller-${cfg.getString("name")}", cfg.getString("class"))
    }
    builder.result()
  }

  case class PollerConfig(name: String, cls: String)
}
