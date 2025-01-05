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
package com.netflix.atlas.webapi

import java.util.concurrent.TimeUnit
import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.ActorLogging
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.norm.NormalizationCache
import com.netflix.atlas.core.validation.ValidationResult
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.netflix.spectator.api.histogram.BucketFunctions

class LocalPublishActor(registry: Registry, db: Database) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.PublishApi.*

  // TODO: This actor is only intended to work with MemoryDatabase, but the binding is
  // setup for the Database interface.
  private val processor =
    new LocalPublishActor.DatapointProcessor(registry, db.asInstanceOf[MemoryDatabase])

  // Number of invalid datapoints received
  private val numInvalid = registry.createId("atlas.db.numInvalid")

  def receive: Receive = {
    case req @ PublishRequest(Nil, Nil, _, _) =>
      req.complete(DiagnosticMessage.error(StatusCodes.BadRequest, "empty payload"))
    case req @ PublishRequest(Nil, failures, _, _) =>
      updateStats(failures)
      val msg = FailureMessage.error(failures)
      sendError(req, StatusCodes.BadRequest, msg)
    case req @ PublishRequest(values, Nil, _, _) =>
      processor.update(values)
      req.complete(HttpResponse(StatusCodes.OK))
    case req @ PublishRequest(values, failures, _, _) =>
      processor.update(values)
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
      case ValidationResult.Fail(error, _, _) =>
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
    */
  private[webapi] class DatapointProcessor(registry: Registry, memDb: MemoryDatabase) {

    // Track the ages of data flowing into the system. Data is expected to arrive quickly and
    // should hit the backend within the step interval used.
    private val numReceived = {
      val f = BucketFunctions.age(DefaultSettings.stepSize, TimeUnit.MILLISECONDS)
      BucketCounter.get(registry, registry.createId("atlas.db.numMetricsReceived"), f)
    }

    private val cache = new NormalizationCache(DefaultSettings.stepSize, memDb.update)

    def update(vs: List[DatapointTuple]): Unit = {
      val now = registry.clock().wallTime()
      vs.foreach { v =>
        numReceived.record(now - v.timestamp)
        v.tags.get(TagKey.dsType) match {
          case Some("counter") => cache.updateCounter(v.id, v.tags, v.timestamp, v.value)
          case Some("gauge")   => cache.updateGauge(v.id, v.tags, v.timestamp, v.value)
          case Some("rate")    => cache.updateRate(v.id, v.tags, v.timestamp, v.value)
          case Some("sum")     => cache.updateSum(v.id, v.tags, v.timestamp, v.value)
          case _               => cache.updateRate(v.id, v.tags, v.timestamp, v.value)
        }
      }
    }

  }
}
