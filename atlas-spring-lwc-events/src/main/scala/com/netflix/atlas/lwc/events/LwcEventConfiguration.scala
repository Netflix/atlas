package com.netflix.atlas.lwc.events

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

import java.util.Optional

@Configuration
class LwcEventConfiguration {

  import scala.jdk.CollectionConverters._

  @Bean
  def lwcEventClient(config: Optional[Config]): LwcEventClient = {
    val c = config.orElseGet(() => ConfigFactory.load())
    val logger = LoggerFactory.getLogger(classOf[LwcEventClient])
    val subs = toSubscriptions(c)
    LwcEventClient(subs, logger.info)
  }

  private def toSubscriptions(config: Config): Subscriptions = {
    val cfg = config.getConfig("atlas.lwc.events.subscriptions")
    Subscriptions(
      passThrough = toSubscriptions(cfg.getConfigList("pass-through")),
      analytics = toSubscriptions(cfg.getConfigList("analytics")),
      tracePassThrough = toSubscriptions(cfg.getConfigList("trace-pass-through")),
      traceAnalytics = toSubscriptions(cfg.getConfigList("trace-analytics"))
    )
  }

  private def toSubscriptions(configs: java.util.List[_ <: Config]): List[Subscription] = {
    configs.asScala.toList.map { c =>
      val step = if (c.hasPath("step")) c.getDuration("step").toMillis else 60_000L
      Subscription(c.getString("id"), step, c.getString("expression"))
    }
  }
}
