/*
 * Copyright 2014-2017 Netflix, Inc.
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
import akka.actor.ActorRef
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
import spray.http.HttpEntity
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCode
import spray.http.StatusCodes


class LocalPublishActor(registry: Registry, db: Database) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.PublishApi._

  // TODO: This actor is only intended to work with MemoryDatabase, but the binding is
  // setup for the Database interface.
  private val memDb = db.asInstanceOf[MemoryDatabase]

  // Track the ages of data flowing into the system. Data is expected to arrive quickly and
  // should hit the backend within the step interval used.
  private val numReceived = {
    val f = BucketFunctions.age(DefaultSettings.stepSize, TimeUnit.MILLISECONDS)
    BucketCounter.get(registry, registry.createId("atlas.db.numMetricsReceived"), f)
  }

  // Number of invalid datapoints received
  private val numInvalid = registry.createId("atlas.db.numInvalid")

  private val cache = new NormalizationCache(DefaultSettings.stepSize, memDb.update)

  def receive = {
    case PublishRequest(Nil, Nil) =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "empty payload")
    case PublishRequest(Nil, failures) =>
      updateStats(failures)
      val msg = FailureMessage.error(failures)
      sendError(sender(), StatusCodes.BadRequest, msg)
    case PublishRequest(values, Nil) =>
      update(values)
      sender() ! HttpResponse(StatusCodes.OK)
    case PublishRequest(values, failures) =>
      update(values)
      updateStats(failures)
      val msg = FailureMessage.partial(failures)
      sendError(sender(), StatusCodes.Accepted, msg)
  }

  private def sendError(ref: ActorRef, status: StatusCode, msg: FailureMessage): Unit = {
    val entity = HttpEntity(MediaTypes.`application/json`, msg.toJson)
    ref ! HttpResponse(status = status, entity = entity)
  }

  private def updateStats(failures: List[ValidationResult]): Unit = {
    failures.foreach {
      case ValidationResult.Pass           => // Ignored
      case ValidationResult.Fail(error, _) =>
        registry.counter(numInvalid.withTag("error", error)).increment()
    }
  }

  private def update(vs: List[Datapoint]): Unit = {
    val now = System.currentTimeMillis()
    vs.foreach { v =>
      numReceived.record(now - v.timestamp)
      v.tags.get(TagKey.dsType) match {
        case Some("counter") => cache.updateCounter(v)
        case Some("gauge")   => cache.updateGauge(v)
        case Some("rate")    => cache.updateRate(v)
        case _               => cache.updateRate(v)
      }
    }
  }
}

