/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import javax.inject.Inject
import javax.inject.Singleton

import akka.actor.ActorSystem
import com.netflix.atlas.json.Json
import com.netflix.iep.service.AbstractService
import com.netflix.spectator.api.Registry
import com.redis.RedisClient
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

/**
  * Lwcapi Startup Service.
  *
  * @param config
  *     System configuration used for main server settings. In particular `atlas.lwcapi.redis.*`
  *     is used for setting the redis host and port.
  * @param registry
  *     Metrics registry for reporting server stats.
  */
@Singleton
class LwcapiStartupServer @Inject() (config: Config,
                                     registry: Registry,
                                     splitter: ExpressionSplitter,
                                     alertmap: ExpressionsDatabase)

  extends AbstractService with StrictLogging {

  private val host = ApiSettings.redisHost
  private val port = ApiSettings.redisPort
  private val keyPrefix = ApiSettings.redisKeyPrefix + "."

  private val updatesId = registry.createId("atlas.lwcapi.db.updates")
  private val connectsId = registry.createId("atlas.lwcapi.redis.connects")
  private val connectRetriesId = registry.createId("atlas.lwcapi.redis.connectRetries")

  protected def startImpl(): Unit = {
    logger.info(s"Loading redis data from $host:$port")

    val client = connect("load").get

    logger.info("Connected to redis.  Loading data...")

    var cursor: Int = 0
    var done: Boolean = false
    var count: Long = 0

    while (!done) {
      val ret = client.scan(cursor)
      if (ret.isDefined) {
        cursor = ret.get._1.getOrElse(0)
        val entries = ret.get._2.getOrElse(List())
        entries.foreach(keyOrNone => {
          val key = keyOrNone.getOrElse("")
          if (key.startsWith(keyPrefix)) {
            try {
              count += 1
              val json = client.get(key)
              val entry = Json.decode[ExpressionDatabaseActor.RedisRequest](json.get)
              val split = splitter.split(entry.expression)
              alertmap.addExpr(split)
              registry.counter(updatesId.withTag("source", "load").withTag("action", "sub")).increment()
            } catch {
              case NonFatal(ex) => logger.error(s"Error loading redis key $key", ex)
            }
          }
        })
      }
      done = cursor == 0
    }

    logger.info(s"Loading complete. $count entries loaded.")
  }

  protected def stopImpl(): Unit = {
  }

  private def connect(source: String): Option[RedisClient] = {
    var tries = 1
    var success = false
    while (!success) {
      try {
        logger.info(s"Connecting to redis($source), tries $tries")

        registry.counter(connectsId.withTag("source", source)).increment()
        if (tries != 1) {
          registry.counter(connectRetriesId.withTag("source", source)).increment()
        }
        Thread.sleep(1000)
        val client = new RedisClient(host, port)
        return Some(client)
      } catch {
        case NonFatal(ex) =>
          logger.warn("Connection error: " + ex.getMessage)
          tries += 1
      }
    }
    None
  }
}
