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

import akka.actor.Actor
import com.netflix.atlas.json.{Json, JsonSupport}
import com.netflix.atlas.lwcapi.ExpressionSplitter.SplitResult
import com.netflix.spectator.api.{Id, Spectator}
import com.redis._
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

class ExpressionDatabaseActor @Inject() (splitter: ExpressionSplitter,
                                         alertmap: ExpressionDatabase,
                                         sm: SubscriptionManager) extends Actor with StrictLogging {
  import ExpressionDatabaseActor._

  private val channel = "expressions"
  private var subClient: RedisClient = _
  private var pubClient: RedisClient = _

  private val registry = Spectator.globalRegistry()
  private val updatesId = registry.createId("atlas.lwcapi.db.updates")
  private val connectsId = registry.createId("atlas.lwcapi.redis.connects")
  private val connectRetriesId = registry.createId("atlas.lwcapi.redis.connectRetries")
  private val bytesReadId = registry.createId("atlas.lwcapi.redis.bytesRead")
  private val bytesWrittenId = registry.createId("atlas.lwcapi.redis.bytesWritten")
  private val messagesReadId = registry.createId("atlas.lwcapi.redis.messagesRead")
  private val messagesWrittenId = registry.createId("atlas.lwcapi.redis.messagesWritten")

  private val uuid = GlobalUUID.get

  private val ttl = ApiSettings.redisTTL
  private val host = ApiSettings.redisHost
  private val port = ApiSettings.redisPort
  private val expressionKeyPrefix = ApiSettings.redisExpressionKeyPrefix
  private val subscribeKeyPrefix = ApiSettings.redisSubscribeKeyPrefix

  private val redisCmdExpression = "expr"
  private val redisCmdSubscribe = "sub"
  private val redisCmdUnsubscribe = "unsub"

  restartPubsub()

  def restartPubsub(): Unit = {
    subClient = connect("subscribe").get
    pubClient = connect("publish").get
    logger.info("Pubsub restarted!")
    subClient.subscribe(channel)(redisCallback)
  }

  def redisCallback(pubsub: PubSubMessage) = pubsub match {
    case S(chan, cnt) => logger.info(s"Subscribe to $chan, sub count is now $cnt")
    case U(chan, cnt) => logger.info(s"Unsubscribe from $chan, sub count is now $cnt")
    case E(exc) =>
      logger.error("redis pubsub: exception caught", exc)
      restartPubsub()
    case M(chan, msg) =>
      val split = msg.split(" ", 3)
      processRedisCommand(split(0), split(1), split(2), msg.length)
  }

  def processRedisCommand(cmd: String, originator: String, json: String, len: Long) = {
    if (originator != uuid) {
      cmd match {
        case `redisCmdExpression` =>
          increment_counter(bytesReadId, "pubsub", actionExpression, len)
          increment_counter(messagesReadId, "pubsub", actionExpression)
          processRedisExpression(json)
        case `redisCmdSubscribe` =>
          increment_counter(bytesReadId, "pubsub", actionSubscribe, len)
          increment_counter(messagesReadId, "pubsub", actionSubscribe)
          processRedisSubscribe(json)
        case `redisCmdUnsubscribe` =>
          increment_counter(bytesReadId, "pubsub", actionUnsubscribe, len)
          increment_counter(messagesReadId, "pubsub", actionUnsubscribe)
          processRedisUnsubscribe(json)
      }
    }
  }

  def processRedisExpression(json: String) = {
    val req = RedisExpressionRequest.fromJson(json)
    val split = splitter.split(ExpressionWithFrequency(req.expression, req.frequency))
    increment_counter(updatesId, "pubsub", actionExpression)
    alertmap.addExpr(split)
  }

  def processRedisSubscribe(json: String) = {
    val req = RedisSubscribeRequest.fromJson(json)
    increment_counter(updatesId, "pubsub", actionSubscribe)
    sm.subscribe(req.streamId, req.expId)
  }

  def processRedisUnsubscribe(json: String) = {
    val req = RedisUnsubscribeRequest.fromJson(json)
    increment_counter(updatesId, "pubsub", actionUnsubscribe)
    sm.unsubscribe(req.streamId, req.expId)
  }

  // Todo: how do we handle unsub from all?

  // Todo: how do we handle TTL expiry of session IDs and expressionIDs and expressions?

  def increment_counter(counter: Id, source: String, action: String, value: Long = 1) = {
    registry.counter(counter.withTag("source", source).withTag("action", action)).increment(value)
  }

  def receive = {
    case Expression(split) =>
      increment_counter(updatesId, "local", actionExpression)
      alertmap.addExpr(split)
      publish(RedisExpressionRequest(split.id, split.expression, split.frequency))
    case Subscribe(streamId, expressionId) =>
      increment_counter(updatesId, "local", actionSubscribe)
      sm.subscribe(streamId, expressionId)
      publish(RedisSubscribeRequest(streamId, expressionId))
    case Unsubscribe(streamId, expressionId) =>
      increment_counter(updatesId, "local", actionUnsubscribe)
      sm.unsubscribe(streamId, expressionId)
      publish(RedisUnsubscribeRequest(streamId, expressionId))
  }

  def publish(req: RedisExpressionRequest) = {
    val json = req.toJson
    pubClient.publish(channel, s"$redisCmdExpression $uuid $json")
    val key = s"$expressionKeyPrefix.${req.id}"
    pubClient.psetex(key, ttl, json)
  }

  def publish(req: RedisSubscribeRequest) = {
    val json = req.toJson
    pubClient.publish(channel, s"$redisCmdSubscribe $uuid $json")
    val key = s"$subscribeKeyPrefix.${req.streamId}.${req.expId}"
    pubClient.psetex(key, ttl, true)
  }

  def publish(req: RedisUnsubscribeRequest) = {
    val json = req.toJson
    pubClient.publish(channel, s"$redisCmdUnsubscribe $uuid $json")
    val key = s"$subscribeKeyPrefix.${req.streamId}.${req.expId}"
    pubClient.del(key)
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

object ExpressionDatabaseActor {

  //
  // Commands as sent over the redis pubsub, or stored in the redis key-value store
  //

  case class RedisExpressionRequest(id: String, expression: String, frequency: Long) extends JsonSupport

  object RedisExpressionRequest {
    def fromJson(json: String): RedisExpressionRequest = Json.decode[RedisExpressionRequest](json)
  }

  case class RedisSubscribeRequest(streamId: String, expId: String) extends JsonSupport

  object RedisSubscribeRequest {
    def fromJson(json: String): RedisSubscribeRequest = Json.decode[RedisSubscribeRequest](json)
  }

  case class RedisUnsubscribeRequest(streamId: String, expId: String) extends JsonSupport

  object RedisUnsubscribeRequest {
    def fromJson(json: String): RedisUnsubscribeRequest = Json.decode[RedisUnsubscribeRequest](json)
  }

  //
  // Commands sent via the actor receive method
  //

  case class Expression(split: SplitResult)

  case class Subscribe(streamId: String, expressionId: String)

  case class Unsubscribe(streamId: String, expressionId: String)

  val actionExpression = "expression"
  val actionSubscribe = "subscribe"
  val actionUnsubscribe = "unsubscribe"
}
