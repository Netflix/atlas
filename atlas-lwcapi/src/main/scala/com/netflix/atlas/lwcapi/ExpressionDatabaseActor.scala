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

import akka.actor.{Actor, Cancellable}
import com.netflix.atlas.json.{Json, JsonSupport}
import com.netflix.atlas.lwcapi.ExpressionSplitter.SplitResult
import com.netflix.atlas.lwcapi.StreamApi.SSEGenericJson
import com.netflix.spectator.api.{Id, Registry}
import com.redis._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
import scala.util.control.NonFatal

class ExpressionDatabaseActor @Inject() (splitter: ExpressionSplitter,
                                         alertmap: ExpressionDatabase,
                                         sm: SubscriptionManager,
                                         registry: Registry,
                                         dbMonitor: DatabaseService) extends Actor with StrictLogging {
  import ExpressionDatabaseActor._

  private val channel = ApiSettings.redisPrefixFor("expressions")
  private var subClient: RedisClient = _
  private var pubClient: RedisClient = _

  private val updatesId = registry.createId("atlas.lwcapi.db.updates")
  private val connectsId = registry.createId("atlas.lwcapi.redis.connects")
  private val connectRetriesId = registry.createId("atlas.lwcapi.redis.connectRetries")
  private val bytesReadId = registry.createId("atlas.lwcapi.redis.bytesRead")
  private val bytesWrittenId = registry.createId("atlas.lwcapi.redis.bytesWritten")
  private val messagesReadId = registry.createId("atlas.lwcapi.redis.messagesRead")
  private val messagesWrittenId = registry.createId("atlas.lwcapi.redis.messagesWritten")

  private val actionHeartbeat = "heartbeat"
  private val actionExpression = "expression"
  private val actionSubscribe = "subscribe"
  private val actionUnsubscribe = "unsubscribe"

  private val uuid = GlobalUUID.get

  private val ttl = ApiSettings.redisTTL
  private val host = ApiSettings.redisHost
  private val port = ApiSettings.redisPort

  private val redisCmdHeartbeat = "hb"
  private val redisCmdExpression = "expr"

  private val heartbeatInterval = 10000 // milliseconds

  object TTLState {
    sealed trait EnumVal
    case object Active extends EnumVal
    case object PendingDelete extends EnumVal
    case object NotPresent extends EnumVal
    val allStates = Seq(Active, PendingDelete)
  }

  private val ttlManager = new TTLManager[String]()
  private val ttlState = mutable.Map[String, TTLState.EnumVal]().withDefaultValue(TTLState.NotPresent)

  //
  // refreshTime determines how often each item is retransmitted via pubsub.
  // It must be smaller than ttl so items will not expire out of caches
  // before a refresh of those interested in that data occurs.
  //
  private val refreshTime = Math.max(ttl / 3 * 2, 1)
  private val maxJitter = Math.max(ttl / 8, 1)

  //
  // Track if we should advertise ourselves as healthy.  We must receive something
  // from redis (even if it's just our own transmissions echoed back) to ensure
  // we are connected.
  private var dbComplete = false
  private var firstRedisReceive: Long = 0

  restartPubsub()

  case class Tick()
  private val tickTime = 1.second
  var ticker: Cancellable = context.system.scheduler.scheduleOnce(tickTime) {
    self ! Tick
  }

  def nextTTLWithJitter(now: Long = System.currentTimeMillis()): Long = {
    now + Math.max(refreshTime + maxJitter / 2 + Random.nextInt(maxJitter / 2), refreshTime)
  }

  def nextTTL(now: Long = System.currentTimeMillis()): Long = {
    now + refreshTime
  }

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
      // We will treat any traffic as an indication we are alive
      val now = System.currentTimeMillis()
      if (firstRedisReceive == 0)
        firstRedisReceive = now
      val split = msg.split(" ", 3)
      if (split(1) != uuid) {
        processRedisCommand(now, split(0), split(1), split(2), msg.length)
      }
  }

  case class RedisLog(cmd: String, originator: String, json: JsonSupport) extends JsonSupport

  def logRedisCommand(cmd: String, originator: String, what: String, obj: JsonSupport) = {
    try {
      val actor = sm.registration(":::redis")
      if (actor.isDefined)
        actor.get.actorRef ! SSEGenericJson(what, RedisLog(cmd, originator, obj))
    } catch {
      case NonFatal(ex) => logger.error("Cannot log to redis", ex)
    }
  }

  def processRedisCommand(now: Long, cmd: String, originator: String, json: String, len: Long) = {
    cmd match {
      case `redisCmdExpression` =>
        increment_counter(bytesReadId, "pubsub", actionExpression, len)
        increment_counter(messagesReadId, "pubsub", actionExpression)
        processRedisExpression(cmd, originator, json, now)
      case `redisCmdHeartbeat` =>
        increment_counter(bytesReadId, "pubsub", actionHeartbeat, len)
        increment_counter(messagesReadId, "pubsub", actionHeartbeat, len)
        processRedisHeartbeat(cmd, originator, json, now)
      case x => logger.info(s"Unknown redis command: $cmd $json")
    }
  }

  def processRedisExpression(cmd: String, originator: String, json: String, now: Long) = {
    val req = RedisExpressionRequest.fromJson(json)
    logger.debug(s"pubsub add for expressionId ${req.id}")
    logRedisCommand(cmd, originator, "redisReceive", req)
    if (!alertmap.hasExpr(req.id)) {
      val split = splitter.split(req.expression, req.frequency)
      if (split.queries.size != 1) {
        logger.error(s"Redis found more than one data expression: ${split.queries.size} ${req.expression}")
      } else if (split.expressions.head.id != req.id) {
        logger.error(s"Redis found an expression that mapped into a different id: ${req.id} != ${split.expressions.head.id} for ${req.expression}")
      } else {
        alertmap.addExpr(split.expressions.head, split.queries.head)
      }
    }
    increment_counter(updatesId, "pubsub", actionExpression)
    ttlState(req.id) = TTLState.Active
    ttlManager.touch(req.id, nextTTLWithJitter(now))
  }

  def processRedisHeartbeat(cmd: String, originator: String, json: String, now: Long) = {
    logRedisCommand(cmd, originator, "redisReceive", RedisHeartbeat())
  }

  def increment_counter(counter: Id, source: String, action: String, value: Long = 1) = {
    registry.counter(counter.withTag("source", source).withTag("action", action)).increment(value)
  }

  def receive = {
    case Expression(split) =>
      increment_counter(updatesId, "local", actionExpression)
      split.queries.zip(split.expressions).foreach { case (query, expr) =>
        alertmap.addExpr(expr, query)
        redisPublish(RedisExpressionRequest(expr.id, expr.expression, expr.frequency))
      }
    case Subscribe(streamId, expressionId) =>
      logger.debug(s"Adding sub streamId $streamId expressionID $expressionId")
      increment_counter(updatesId, "local", actionSubscribe)
      sm.subscribe(streamId, expressionId)
    case Unsubscribe(streamId, expressionId) =>
      logger.debug(s"Adding unsub streamId $streamId expressionID $expressionId")
      increment_counter(updatesId, "local", actionUnsubscribe)
      sm.unsubscribe(streamId, expressionId)
    case Tick =>
      checkDbStatus()
      checkTTLs()
      maybeHeartbeat()
      ticker = context.system.scheduler.scheduleOnce(tickTime) {
        self ! Tick
      }
  }

  def redisPublish(req: RedisExpressionRequest) = {
    publish(redisCmdExpression, actionExpression, req)
    ttlManager.touch(req.id, nextTTL())
    ttlState(req.id) = TTLState.Active
  }

  var lastHeartbeated = System.currentTimeMillis()
  def maybeHeartbeat() = {
    val now = System.currentTimeMillis()
    if (lastHeartbeated + heartbeatInterval < now) {
      lastHeartbeated = now
      val heartbeat = RedisHeartbeat()
      publish(redisCmdHeartbeat, actionHeartbeat, heartbeat)
    }
  }

  def publish(cmd: String, action: String, item: RedisJson) = {
    val json = item.toJson
    pubClient.publish(channel, s"$cmd $uuid $json")
    logRedisCommand(cmd, uuid, "redisSend", item)
    increment_counter(bytesWrittenId, "local", action, json.length)
    increment_counter(messagesWrittenId, "local", action)
  }

  def checkDbStatus() = {
    if (!dbComplete) {
      logger.debug("Full redis re-sync pending")
      if (firstRedisReceive == 0) {
        logger.debug("No redis traffic seen yet")
      } else {
        val now = System.currentTimeMillis()
        if (firstRedisReceive + refreshTime < now) {
          logger.debug("Full redis re-sync complete")
          dbComplete = true
          dbMonitor.setState(true)
        }
      }
    }
  }

  // For each entry found, if we still know about it, touch it in redis.
  // We will do this at half the ttl period to be sure we don't let things expire.
  def checkTTLs() = {
    var done = false
    val now = System.currentTimeMillis()
    val targetTime = now - refreshTime
    while (!done) {
      val top = ttlManager.needsTouch(targetTime)
      if (top.isDefined) {
        logger.debug(s"TTL: Checking ${top.get}")
        touchExpression(top.get, now)
      } else {
        done = true
      }
    }
  }

  def touchExpression(id: String, now: Long) = {
    val state = ttlState(id)
    state match {
      case TTLState.NotPresent => // do nothing
        logger.warn(s"TTL: Expression state for $id is strangely NotPresent")
      case TTLState.Active =>
        val subscriberPresent = sm.actorsForExpression(id).nonEmpty
        val split = alertmap.expr(id)
        if (!subscriberPresent || split.isEmpty) {
          ttlState(id) = TTLState.PendingDelete
          logger.debug(s"TTL: Expression state for $id set to PendingDelete")
        } else {
          redisPublish(RedisExpressionRequest(split.get.id, split.get.expression, split.get.frequency))
        }
        ttlManager.touch(id, nextTTL(now))
      case TTLState.PendingDelete =>
        logger.debug(s"TTL: Deleting $id")
        ttlState.remove(id)
        alertmap.delExpr(id)
    }
  }

  private def connect(source: String): Option[RedisClient] = {
    var tries = 1
    while (true) {
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

  override def postStop() = {
    ticker.cancel()
    super.postStop()
  }

  case class TTLItem(flavor: String, id: String) extends Ordered[TTLItem] {
    def compare(other: TTLItem): Int = {
      var ret = id compare other.id
      if (ret == 0) ret = flavor compare other.flavor
      ret
    }
  }
}

object ExpressionDatabaseActor {
  sealed trait RedisJson extends JsonSupport

  private val instanceId = sys.env.getOrElse("EC2_INSTANCE_ID", "unknown")

  //
  // Commands as sent over the redis pubsub, or stored in the redis key-value store
  //

  case class RedisExpressionRequest(id: String, expression: String, frequency: Int) extends RedisJson {
    require(id != null && id.nonEmpty)
    require(expression != null && expression.nonEmpty)
    require(frequency > 0)
  }

  object RedisExpressionRequest {
    def fromJson(json: String): RedisExpressionRequest = Json.decode[RedisExpressionRequest](json)
  }

  case class RedisHeartbeat(instanceId: String = instanceId) extends RedisJson

  //
  // Commands sent via the actor receive method
  //

  case class Expression(split: SplitResult)

  case class Subscribe(streamId: String, expressionId: String)

  case class Unsubscribe(streamId: String, expressionId: String)
}
