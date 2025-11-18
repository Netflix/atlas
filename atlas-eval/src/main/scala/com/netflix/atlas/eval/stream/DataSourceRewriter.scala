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
package com.netflix.atlas.eval.stream

import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.Uri

/**
  * Base trait for a rewriter implementation that can be used to alter the data sources
  * before processing in the stream.
  */
trait DataSourceRewriter {

  def rewrite(context: StreamContext, dss: DataSources): DataSources
}

object DataSourceRewriter {

  /** Create a new instance of the rewriter. */
  def create(config: Config): DataSourceRewriter = {
    val rewriteConfig = config.getConfig("atlas.eval.stream.rewrite")
    rewriteConfig.getString("mode") match {
      case "none"   => NoneRewriter
      case "config" => new ConfigRewriter(rewriteConfig)
      case mode     => throw new IllegalArgumentException(s"unsupported rewrite mode: $mode")
    }
  }

  /**
    * Implementation that does nothing and passes through the original data sources
    * unmodified.
    */
  private object NoneRewriter extends DataSourceRewriter {

    override def rewrite(context: StreamContext, dss: DataSources): DataSources = {
      dss
    }
  }

  /**
    * Helper to perform a simple mapping from a namespace URL parameter to an alternate
    * host.
    */
  private class ConfigRewriter(config: Config) extends DataSourceRewriter {

    import scala.jdk.CollectionConverters.*

    private val namespaces = {
      config
        .getConfigList("namespaces")
        .asScala
        .map { c =>
          val ns = c.getString("namespace")
          val host = c.getString("host")
          ns -> host
        }
        .toMap
    }

    private def rewrite(context: StreamContext, ds: DataSource): Option[DataSource] = {
      val uri = Uri(ds.uri())
      val query = uri.query()
      query.get("ns") match {
        case Some(ns) =>
          namespaces.get(ns) match {
            case Some(host) =>
              val q = query.filterNot(_._1 == "ns")
              val rewrittenUri = uri.withHost(host).withQuery(q).toString()
              Some(new DataSource(ds.id, ds.step, rewrittenUri))
            case None =>
              val msg = DiagnosticMessage.error(s"unsupported namespace: $ns")
              context.dsLogger(ds, msg)
              None
          }
        case None =>
          Some(ds)
      }
    }

    override def rewrite(context: StreamContext, dss: DataSources): DataSources = {
      val sources = dss.sources.asScala.flatMap(ds => rewrite(context, ds)).toSet
      new DataSources(sources.asJava)
    }
  }
}
