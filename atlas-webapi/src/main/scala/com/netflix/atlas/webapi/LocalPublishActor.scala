/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.webapi

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.norm.NormalizationCache
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.netflix.spectator.api.histogram.BucketFunctions
import com.netflix.spectator.api.patterns.PolledMeter

class LocalPublishActor(registry: Registry, db: Database) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.PublishApi._

  // TODO: This actor is only intended to work with MemoryDatabase, but the binding is
  // setup for the Database interface.
  private val processor =
    new LocalPublishActor.DatapointProcessor(registry, db.asInstanceOf[MemoryDatabase])

  // Number of invalid datapoints received
  private val numInvalid = registry.createId("atlas.db.numInvalid")

  def receive: Receive = {
    case req @ PublishRequest(_, Nil, Nil, _, _) =>
      req.complete(DiagnosticMessage.error(StatusCodes.BadRequest, "empty payload"))
    case req @ PublishRequest(_, Nil, failures, _, _) =>
      updateStats(failures)
      val msg = FailureMessage.error(failures)
      sendError(req, StatusCodes.BadRequest, msg)
    case req @ PublishRequest(id, values, Nil, _, _) =>
      processor.update(id, values)
      req.complete(HttpResponse(StatusCodes.OK))
    case req @ PublishRequest(id, values, failures, _, _) =>
      processor.update(id, values)
      updateStats(failures)
      val msg = FailureMessage.partial(failures)
      sendError(req, StatusCodes.Accepted, msg)
  }

  private def sendError(req: PublishRequest, status: StatusCode, msg: FailureMessage): Unit = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    req.complete(HttpResponse(status = status, entity = entity))
  }

  private def updateStats(failures: List[ValidationResult]): Unit = {
    failures.foreach {
      case ValidationResult.Pass => // Ignored
      case ValidationResult.Fail(error, _) =>
        registry.counter(numInvalid.withTag("error", error)).increment()
    }
  }
}

object LocalPublishActor {

  /**
    * Process datapoints from publish requests and feed into the in-memory database.
    *
    * @param registry
    *     Registry for keeping track of metrics.
    * @param memDb
    *     Database that will receive the final processed datapoints.
    * @param maxMessageIds
    *     Maximum number of message ids to track. If this limit is exceeded, then it
    *     is possible for duplicate messages to get processed which may lead to over
    *     counting if rollups are being used.
    */
  private[webapi] class DatapointProcessor(
    registry: Registry,
    memDb: MemoryDatabase,
    maxMessageIds: Int = 1000000
  ) {

    // Track the ages of data flowing into the system. Data is expected to arrive quickly and
    // should hit the backend within the step interval used.
    private val numReceived = {
      val f = BucketFunctions.age(DefaultSettings.stepSize, TimeUnit.MILLISECONDS)
      BucketCounter.get(registry, registry.createId("atlas.db.numMetricsReceived"), f)
    }

    // Track the messages that have already been processed. This is primarily to avoid over
    // counting when computing inline rollups.
    private val messageIds = new java.util.LinkedHashMap[String, String](16, 0.75f, true) {

      override def removeEldestEntry(eldest: java.util.Map.Entry[String, String]): Boolean = {
        size() > maxMessageIds
      }
    }

    // Track the size of the message ids buffer
    PolledMeter
      .using(registry)
      .withName("atlas.db.messageIdSetSize")
      .monitorSize(messageIds)

    // Number of datapoints that are deduped
    private val numDeduped = registry.counter("atlas.db.numDeduped")

    private val cache = new NormalizationCache(DefaultSettings.stepSize, memDb.update)

    def update(id: String, vs: List[Datapoint]): Unit = {

      // Duplicate messages should be rare, so we assume that put is the common
      // case and do that up front rather than doing a separate check to see if
      // the set contains the key
      if (id == null || messageIds.put(id, id) == null) {
        val now = registry.clock().wallTime()
        vs.foreach { v =>
          numReceived.record(now - v.timestamp)
          if (v.tags.contains(TagKey.rollup)) {
            memDb.rollup(v.copy(tags = v.tags - TagKey.rollup))
          } else {
            v.tags.get(TagKey.dsType) match {
              case Some("counter") => cache.updateCounter(v)
              case Some("gauge")   => cache.updateGauge(v)
              case Some("rate")    => cache.updateRate(v)
              case _               => cache.updateRate(v)
            }
          }
        }
      } else {
        numDeduped.increment()
      }
    }

  }
}
