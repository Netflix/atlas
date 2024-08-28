/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.HostSource.unzipIfNeeded
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.OpportunisticEC.ec
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.atlas.pekko.StreamOps
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.stream.scaladsl.BroadcastHub
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.RetryFlow
import org.apache.pekko.stream.scaladsl.Source

import java.io.ByteArrayOutputStream
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Using

class DataSourceRewriter(
  config: Config,
  registry: Registry,
  implicit val system: ActorSystem
) extends StrictLogging {

  private val rewriteConfig = config.getConfig("atlas.eval.stream.rewrite")
  private val enabled = rewriteConfig.getBoolean("enabled")
  private val rewriteUri = rewriteConfig.getString("uri")

  if (enabled) {
    logger.info(s"rewriting enabled with uri: $rewriteUri")
  } else {
    logger.info(s"rewriting is disabled")
  }

  private val client = PekkoHttpClient
    .create("datasource-rewrite", system)
    .superPool[List[DataSource]]()

  private val rewriteCache = new ConcurrentHashMap[String, String]()

  private val rewriteSuccess = registry.counter("atlas.eval.stream.rewrite.success")
  private val rewriteFailures = registry.createId("atlas.eval.stream.rewrite.failures")
  private val rewriteCacheHits = registry.counter("atlas.eval.stream.rewrite.cache", "id", "hits")

  private val rewriteCacheMisses =
    registry.counter("atlas.eval.stream.rewrite.cache", "id", "misses")

  def rewrite(
    context: StreamContext,
    keepRetrying: Boolean = true
  ): Flow[DataSources, DataSources, NotUsed] = {
    rewrite(client, context, keepRetrying)
  }

  def rewrite(
    client: SuperPoolClient,
    context: StreamContext,
    keepRetrying: Boolean
  ): Flow[DataSources, DataSources, NotUsed] = {
    if (!enabled) {
      return Flow[DataSources]
    }

    val (cachedQueue, cachedSource) = StreamOps
      .blockingQueue[DataSources](registry, "cachedRewrites", 1)
      .toMat(BroadcastHub.sink(1))(Keep.both)
      .run()
    var sentCacheData = false

    val retryFlow = RetryFlow
      .withBackoff(
        minBackoff = 100.milliseconds,
        maxBackoff = 5.second,
        randomFactor = 0.35,
        maxRetries = if (keepRetrying) -1 else 0,
        flow = httpFlow(client, context)
      ) {
        case (original, resp) =>
          resp match {
            case Success(_) => None
            case Failure(ex) =>
              val (request, dsl) = original
              logger.debug("Retrying the rewrite request due to error", ex)
              if (!sentCacheData) {
                if (!cachedQueue.offer(returnFromCache(dsl))) {
                  // note that this should never happen.
                  logger.error("Unable to send cached results to queue.")
                } else {
                  sentCacheData = true
                }
              }
              Some(request -> dsl)
          }

      }
      .watchTermination() { (_, f) =>
        f.onComplete { _ =>
          cachedQueue.complete()
        }
      }

    Flow[DataSources]
      .map(_.sources().asScala.toList)
      .map(dsl => constructRequest(dsl) -> dsl)
      .via(retryFlow)
      .filter(_.isSuccess)
      .map {
        // reset the cached flag
        sentCacheData = false
        _.get
      }
      .merge(cachedSource)
  }

  private[stream] def httpFlow(client: SuperPoolClient, context: StreamContext) = {
    Flow[(HttpRequest, List[DataSource])]
      .via(client)
      .flatMapConcat {
        case (Success(resp), dsl) =>
          unzipIfNeeded(resp)
            .map(_.utf8String)
            .map { body =>
              resp.status match {
                case StatusCodes.OK =>
                  val rewrites = List.newBuilder[DataSource]
                  Json
                    .decode[List[Rewrite]](body)
                    .zip(dsl)
                    .map {
                      case (r, ds) =>
                        if (!r.status.equals("OK")) {
                          val msg =
                            DiagnosticMessage.error(s"failed rewrite of ${ds.uri()}: ${r.message}")
                          context.dsLogger(ds, msg)
                        } else {
                          rewriteCache.put(ds.uri(), r.rewrite)
                          rewrites += new DataSource(ds.id, ds.step(), r.rewrite)
                        }
                    }
                    .toArray
                  // NOTE: We're assuming that the number of items returned will be the same as the
                  // number of uris sent to the rewrite service. If they differ, data sources may be
                  // mapped to IDs and steps incorrectly.
                  rewriteSuccess.increment()
                  Success(DataSources.of(rewrites.result().toArray: _*))
                case _ =>
                  logger.error(
                    "Error from rewrite service. status={}, resp={}",
                    resp.status,
                    body
                  )
                  registry
                    .counter(
                      rewriteFailures.withTags("status", resp.status.toString(), "exception", "NA")
                    )
                    .increment()
                  Failure(
                    new RuntimeException(
                      s"Error from rewrite service. status=${resp.status}, resp=$body"
                    )
                  )
              }
            }
        case (Failure(ex), _) =>
          logger.error("Failure from rewrite service", ex)
          registry
            .counter(
              rewriteFailures.withTags("status", "0", "exception", ex.getClass.getSimpleName)
            )
            .increment()
          Source.single(Failure(ex))
      }
  }

  private[stream] def returnFromCache(dsl: List[DataSource]): DataSources = {
    val rewrites = dsl.flatMap { ds =>
      val rewrite = rewriteCache.get(ds.uri())
      if (rewrite == null) {
        rewriteCacheMisses.increment()
        None
      } else {
        rewriteCacheHits.increment()
        Some(new DataSource(ds.id, ds.step(), rewrite))
      }
    }
    DataSources.of(rewrites*)
  }

  private[stream] def constructRequest(dss: List[DataSource]): HttpRequest = {
    val baos = new ByteArrayOutputStream
    Using(Json.newJsonGenerator(baos)) { json =>
      json.writeStartArray()
      dss.foreach(s => json.writeString(s.uri()))
      json.writeEndArray()
    }
    HttpRequest(
      uri = rewriteUri,
      method = HttpMethods.POST,
      entity = HttpEntity(ContentTypes.`application/json`, baos.toByteArray)
    )
  }

}

case class Rewrite(status: String, rewrite: String, original: String, message: String)
