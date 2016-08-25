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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.netflix.atlas.json.Json
import com.redis._

class ExpressionDatabaseActor extends Actor with ActorLogging with CatchSafely {
  import ExpressionDatabaseActor._

  private val channel = "expressions"
  private var subClient: RedisClient = _
  private var pubClient: RedisClient = _

  private val uuid = java.util.UUID.randomUUID.toString

  restartPubsub()

  def restartPubsub(): Unit = {
    var tries = 1
    var success = false
    while (!success) {
      try {
        log.info(s"Restarting pubsub, tries $tries")

        Thread.sleep(1000)
        subClient = new RedisClient(ApiSettings.redisHost, ApiSettings.redisPort)
        pubClient = new RedisClient(ApiSettings.redisHost, ApiSettings.redisPort)
        success = true
      } catch safely {
        case ex: Throwable =>
          log.warning("Connection error: " + ex.getMessage)
          tries += 1
      }
    }
    log.info("Pubsub restarted!")
    subClient.subscribe(channel)(redisCallback)
  }

  def redisCallback(pubsub: PubSubMessage) = pubsub match {
    case S(chan, cnt) => log.info(s"Subscribed from $chan, sub count is now $cnt")
    case U(chan, cnt) => log.info(s"Unsubscribed from $chan, sub count is now $cnt")
    case E(exc) => {
      log.error(exc, "redis pubsub: exception caught")
      restartPubsub()
    }
    case M(chan, msg) =>
      val request = Json.decode[RedisRequest](msg)
      if (request.uuid != uuid) {
        val action = request.action
        val expression = request.expression
        val cluster = request.cluster
        log.info(s"PubSub received $action in $cluster for $expression")
        action match {
          case "add" => AlertMap.globalAlertMap.addExpr(cluster, expression)
          case "delete" => AlertMap.globalAlertMap.delExpr(cluster, expression)
        }
      }
  }

  def receive = {
    case Publish(cluster, expression) =>
      log.info(s"PubSub add in $cluster for $expression")
      AlertMap.globalAlertMap.addExpr(cluster, expression)
      val json = Json.encode(RedisRequest(cluster, expression, uuid, "add"))
      pubClient.publish(channel, json)
    case Unpublish(cluster, expression) =>
      log.info(s"PubSub delete in $cluster for $expression")
      AlertMap.globalAlertMap.delExpr(cluster, expression)
      val json = Json.encode(RedisRequest(cluster, expression, uuid, "delete"))
      pubClient.publish(channel, json)
  }
}

object ExpressionDatabaseActor {
  case class RedisRequest(cluster: String, expression: ExpressionWithFrequency, uuid: String, action: String)
  case class Publish(cluster: String, expression: ExpressionWithFrequency)
  case class Unpublish(cluster: String, expression: ExpressionWithFrequency)
}
