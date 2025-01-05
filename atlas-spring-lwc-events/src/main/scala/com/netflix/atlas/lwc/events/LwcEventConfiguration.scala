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
package com.netflix.atlas.lwc.events

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

import java.util.Optional

@Configuration
class LwcEventConfiguration {

  import scala.jdk.CollectionConverters.*

  @Bean
  def lwcEventClient(registry: Optional[Registry], config: Optional[Config]): LwcEventClient = {
    val r = registry.orElseGet(() => new NoopRegistry)
    val c = config.orElseGet(() => ConfigFactory.load())
    if (c.getBoolean("atlas.lwc.events.enabled")) {
      val client = new RemoteLwcEventClient(r, c)
      client.start()
      client
    } else {
      val logger = LoggerFactory.getLogger(classOf[LwcEventClient])
      val subs = toSubscriptions(c)
      LwcEventClient(subs, logger.info)
    }
  }

  private def toSubscriptions(config: Config): Subscriptions = {
    val cfg = config.getConfig("atlas.lwc.events.subscriptions")
    Subscriptions(
      events = toSubscriptions(cfg.getConfigList("events"), Subscriptions.Events),
      timeSeries = toSubscriptions(cfg.getConfigList("time-series"), Subscriptions.TimeSeries),
      traceEvents = toSubscriptions(cfg.getConfigList("trace-events"), Subscriptions.TraceEvents),
      traceTimeSeries =
        toSubscriptions(cfg.getConfigList("trace-time-series"), Subscriptions.TraceTimeSeries)
    )
  }

  private def toSubscriptions(
    configs: java.util.List[? <: Config],
    exprType: String
  ): List[Subscription] = {
    configs.asScala.toList.map { c =>
      val step = if (c.hasPath("step")) c.getDuration("step").toMillis else 60_000L
      Subscription(c.getString("id"), step, c.getString("expression"), exprType)
    }
  }
}
