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

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Spectator
import com.redis._

class ExpressionDatabaseActor extends Actor with ActorLogging with CatchSafely {
  import ExpressionDatabaseActor._

  private val channel = "expressions"
  private var subClient: RedisClient = _
  private var pubClient: RedisClient = _

  private val registry = Spectator.globalRegistry()
  private val updatesId = registry.createId("atlas.lwcapi.expressionDatabase.updates")
  private val connectsId = registry.createId("atlas.lwcapi.expressionDatabase.connects")
  private val connectRetriesId = registry.createId("atlas.lwcapi.expressionDatabase.connectRetries")

  private val uuid = java.util.UUID.randomUUID.toString

  restartPubsub()

  def restartPubsub(): Unit = {
    var tries = 1
    var success = false
    while (!success) {
      try {
        log.info(s"Restarting pubsub, tries $tries")

        registry.counter(connectsId).increment()
        if (tries != 1) {
          registry.counter(connectRetriesId).increment()
        }
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
        log.info(s"PubSub received $action for $expression")
        action match {
          case "add" =>
            registry.counter(updatesId.withTag("source", "remote").withTag("type", "add")).increment()
            AlertMap.globalAlertMap.addExpr(expression)
          case "delete" =>
            registry.counter(updatesId.withTag("source", "remote").withTag("type", "delete")).increment()
            AlertMap.globalAlertMap.delExpr(expression)
        }
      }
  }

  def receive = {
    case Publish(expression) =>
      log.info(s"PubSub add for $expression")
      AlertMap.globalAlertMap.addExpr(expression)
      val json = Json.encode(RedisRequest(expression, uuid, "add"))
      pubClient.publish(channel, json)
      recordUpdate(json)
      registry.counter(updatesId.withTag("source", "local").withTag("type", "add")).increment()
    case Unpublish(expression) =>
      log.info(s"PubSub delete for $expression")
      AlertMap.globalAlertMap.delExpr(expression)
      val json = Json.encode(RedisRequest(expression, uuid, "delete"))
      pubClient.publish(channel, json)
      registry.counter(updatesId.withTag("source", "local").withTag("type", "delete")).increment()
  }

  def recordUpdate(json: String) = {
    val List(expiry, keyindex) = computeTimes(System.currentTimeMillis())
    val keyname = s"expressions.$keyindex"
    val count = pubClient.sadd(keyname, json)
    if (count.isDefined && count.get == 1) {
      pubClient.pexpireat(keyname, expiry)
    }
  }

  def computeTimes(now: Long): List[Long] = {
    List(
      now / 60000 * 60000 + 600000, // 10 minutes starting on the minute boundary
      now / 60000 // used for the key name
    )
  }
}

object ExpressionDatabaseActor {
  case class RedisRequest(expression: ExpressionWithFrequency, uuid: String, action: String)
  case class Publish(expression: ExpressionWithFrequency)
  case class Unpublish(expression: ExpressionWithFrequency)
}
