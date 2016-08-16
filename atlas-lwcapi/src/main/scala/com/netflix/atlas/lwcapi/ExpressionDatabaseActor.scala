package com.netflix.atlas.lwcapi

import akka.actor.{Actor, ActorLogging, Props}
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Registry
import com.redis._

class ExpressionDatabaseActor(registry: Registry, channel: String) extends Actor with ActorLogging {
  import ExpressionDatabaseActor._

  private val subClient = new RedisClient(ApiSettings.redisHost, ApiSettings.redisPort)
  private val pubClient = new RedisClient(ApiSettings.redisHost, ApiSettings.redisPort)

  private val subscriber = context.actorOf(Props(new Subscriber(subClient)))

  val uuid = java.util.UUID.randomUUID.toString

  subscriber ! Register(redisCallback)
  val channels = Array("expressions")
  subscriber ! Subscribe(channels)

  def redisCallback(pubsub: PubSubMessage) = pubsub match {
    // XXXMLG TODO should log
    case S(chan, cnt) => println("Subscribed to " + chan + " and count = " + cnt)
    case U(chan, cnt) => println("Unsubscribed to " + chan + " and count = " + cnt)
    case E(exc) => println("EXCEPTION: " + exc)
    case M(chan, msg) =>
      val request = Json.decode[RedisRequest](msg)
      if (request.uuid != uuid) {
        //println("RECEIVED:\n" + request)
        request.action match {
          case "add" => AlertMap.globalAlertMap.addExpr(request.expression)
          case "delete" => AlertMap.globalAlertMap.delExpr(request.expression)
        }
      }
  }

  def receive = {
    case Publish(expression) =>
      AlertMap.globalAlertMap.addExpr(expression)
      val json = Json.encode(RedisRequest(expression, uuid, "add"))
      pubClient.publish(channel, json)
    case Unpublish(expression) =>
      AlertMap.globalAlertMap.delExpr(expression)
      val json = Json.encode(RedisRequest(expression, uuid, "delete"))
      pubClient.publish(channel, json)
  }
}

object ExpressionDatabaseActor {
  case class RedisRequest(expression: ExpressionWithFrequency, uuid: String, action: String)
  case class Publish(expression: ExpressionWithFrequency)
  case class Unpublish(expression: ExpressionWithFrequency)

  def props(registry: Registry, channel: String) = Props(new ExpressionDatabaseActor(registry, channel))
}
