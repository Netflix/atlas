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
import com.netflix.atlas.pekko.PekkoHttpClient
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.stream.scaladsl.Flow

import java.io.ByteArrayOutputStream
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Using

class DataSourceRewriter(config: Config, implicit val system: ActorSystem) extends StrictLogging {

  private val (enabled, rewriteUrl) = {
    val enabled = config.hasPath("atlas.eval.stream.rewrite-url")
    val url = if (enabled) config.getString("atlas.eval.stream.rewrite-url") else ""
    if (enabled) {
      logger.info(s"Rewriting enabled with url: ${url}")
    } else {
      logger.info("Rewriting is disabled")
    }
    (enabled, url)
  }

  private val client = PekkoHttpClient
    .create("datasource-rewrite", system)
    .superPool[List[DataSource]]()

  def rewrite(context: StreamContext): Flow[DataSources, DataSources, NotUsed] = {
    rewrite(client, context)
  }

  def rewrite(
    client: SuperPoolClient,
    context: StreamContext
  ): Flow[DataSources, DataSources, NotUsed] = {
    if (!enabled) {
      return Flow[DataSources]
    }

    Flow[DataSources]
      .map(_.sources().asScala.toList)
      .map(dsl => constructRequest(dsl) -> dsl)
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
                          rewrites += new DataSource(ds.id, ds.step(), r.rewrite)
                        }
                    }
                    .toArray
                  // NOTE: We're assuming that the number of items returned will be the same as the
                  // number of uris sent to the rewrite service. If they differ, data sources may be
                  // mapped to IDs and steps incorrectly.
                  DataSources.of(rewrites.result().toArray: _*)
                case _ =>
                  logger.error(
                    "Exception from rewrite service. status={}, resp={}",
                    resp.status,
                    body
                  )
                  throw new RuntimeException(body)
              }
            }
        case (Failure(ex), _) =>
          throw ex
      }
  }

  private[stream] def constructRequest(dss: List[DataSource]): HttpRequest = {
    val baos = new ByteArrayOutputStream
    Using(Json.newJsonGenerator(baos)) { json =>
      json.writeStartArray()
      dss.foreach(s => json.writeString(s.uri()))
      json.writeEndArray()
    }
    HttpRequest(
      uri = rewriteUrl,
      method = HttpMethods.POST,
      entity = HttpEntity(ContentTypes.`application/json`, baos.toByteArray)
    )
  }

}

case class Rewrite(status: String, rewrite: String, original: String, message: String)
