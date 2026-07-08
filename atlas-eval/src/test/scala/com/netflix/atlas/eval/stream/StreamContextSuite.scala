/*
 * Copyright 2014-2026 Netflix, Inc.
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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Materializer
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json3.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import scala.collection.mutable

class StreamContextSuite extends FunSuite {

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val materializer: Materializer = Materializer(system)

  private def newContext: StreamContext =
    new StreamContext(ConfigFactory.load(), materializer, dsLogger = DataSourceLogger.Noop)

  // A log-events data source (the "logs.prod" host in the incident) and a time-series data
  // source on a different host (the "atlas" metrics hosts).
  private val eventsUri = "http://logs/api/v1/events?q=name,foo,:eq,(,message,),:table"
  private val eventsDs = new DataSource("logs", java.time.Duration.ofMinutes(1), eventsUri)
  private val metricsUri = "http://atlas/api/v1/graph?q=name,cpu,:eq,:sum"
  private val metricsDs = new DataSource("metrics", java.time.Duration.ofMinutes(1), metricsUri)

  private val msg = DiagnosticMessage.info("test")

  test("messagesForDataSource finds an expression from the active data sources") {
    val context = newContext
    context.setDataSources("logs", DataSources.of(eventsDs))
    val expr = context.interpreter.dataExprs(Uri(eventsUri)).head
    assertEquals(context.messagesForDataSource("logs", expr, msg).map(_.id), List("logs"))
  }

  // createStreamsFlow groups data sources by host and runs a per-host processor over a single
  // shared StreamContext, so each host substream calls setDataSources with only its host's data
  // sources. State must be partitioned by host so a second host does not clobber the first and
  // silently drop messages for the clobbered host.
  test("data sources for one host survive setDataSources for another host") {
    val context = newContext
    val eventExpr = context.interpreter.dataExprs(Uri(eventsUri)).head

    // Host A substream registers the events data source.
    context.setDataSources("logs", DataSources.of(eventsDs))
    assertEquals(context.messagesForDataSource("logs", eventExpr, msg).map(_.id), List("logs"))

    // Host B substream registers its own time-series data source on a different host.
    context.setDataSources("atlas", DataSources.of(metricsDs))

    // The events data source must still be resolvable on its host.
    assertEquals(context.messagesForDataSource("logs", eventExpr, msg).map(_.id), List("logs"))
  }

  // Two hosts with the identical expression string must not cross-deliver: an event produced on
  // one host should only reach that host's data source.
  test("messagesForDataSource is scoped to the host that produced the data") {
    val context = newContext
    val sharedUri = "http://logs/api/v1/events?q=name,foo,:eq,(,message,),:table"
    val expr = context.interpreter.dataExprs(Uri(sharedUri)).head
    val dsA = new DataSource("a", java.time.Duration.ofMinutes(1), sharedUri)
    val dsB = new DataSource("b", java.time.Duration.ofMinutes(1), sharedUri)

    context.setDataSources("hostA", DataSources.of(dsA))
    context.setDataSources("hostB", DataSources.of(dsB))

    // An event produced on hostA is delivered only to hostA's data source, not hostB's.
    assertEquals(context.messagesForDataSource("hostA", expr, msg).map(_.id), List("a"))
    assertEquals(context.messagesForDataSource("hostB", expr, msg).map(_.id), List("b"))
  }

  // An expression with no data source on the requested host is dropped (and counted), even if
  // another host has it.
  test("messagesForDataSource drops when the host has no matching data source") {
    val context = newContext
    val eventExpr = context.interpreter.dataExprs(Uri(eventsUri)).head
    context.setDataSources("logs", DataSources.of(eventsDs))
    assertEquals(context.messagesForDataSource("atlas", eventExpr, msg), Nil)
    assertEquals(context.messagesForDataSource("unknown-host", eventExpr, msg), Nil)
  }

  // logDatapointsExceeded on the streaming path (which has the host) must notify only the host
  // that exceeded the limit, not another host that happens to use the same expression. The
  // host-agnostic overload notifies all hosts (used by the combined datapoint processor).
  test("logDatapointsExceeded is scoped to the host, with a union overload") {
    val logged = mutable.ListBuffer.empty[String]
    val capturing = new DataSourceLogger {
      override def apply(ds: DataSource, msg: JsonSupport): Unit = logged += ds.id()
      override def close(): Unit = ()
    }
    val context = new StreamContext(ConfigFactory.load(), materializer, dsLogger = capturing)
    val sharedUri = "http://logs/api/v1/events?q=name,foo,:eq,(,message,),:table"
    val expr = context.interpreter.dataExprs(Uri(sharedUri)).head
    context.setDataSources(
      "hostA",
      DataSources.of(new DataSource("a", java.time.Duration.ofMinutes(1), sharedUri))
    )
    context.setDataSources(
      "hostB",
      DataSources.of(new DataSource("b", java.time.Duration.ofMinutes(1), sharedUri))
    )

    context.logDatapointsExceeded("hostA", 0L, expr)
    assertEquals(logged.toList, List("a"))

    logged.clear()
    context.logDatapointsExceeded(0L, expr)
    assertEquals(logged.toList.sorted, List("a", "b"))
  }
}
