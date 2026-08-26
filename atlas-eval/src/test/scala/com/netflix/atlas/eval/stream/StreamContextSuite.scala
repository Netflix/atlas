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
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json3.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

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

  // Hi-res data sources must restrict name and app. The check used to materialize the
  // disjunctive normal form, which is exponential in the number of `:or` clauses; it is now
  // computed over the structure of the query. The tests below pin the behavior and check it
  // against the previous definition.

  // Validation does not mutate the context, so a single instance can be shared. Creating one
  // builds the full vocabulary for two interpreters plus a grapher, which is not worth
  // repeating per assertion.
  private lazy val hiResContext: StreamContext = newContext

  /** Message for the validation failure, or None if the data source is valid. */
  private def rejection(query: String, step: java.time.Duration): Option[String] = {
    val uri = s"http://localhost/api/v1/graph?q=$query"
    val ds = new DataSource("test", step, uri)
    hiResContext.validateDataSource(ds).failed.toOption.map(_.getMessage)
  }

  private def hiResRejection(query: String): Option[String] =
    rejection(query, java.time.Duration.ofSeconds(5))

  private def hiResIsValid(query: String): Boolean = hiResRejection(query).isEmpty

  /**
    * Random query used to check the structural checks against the definition based on the
    * disjunctive normal form.
    */
  private def genQuery(random: scala.util.Random, leaves: Vector[Query], depth: Int): Query = {
    def sub: Query = genQuery(random, leaves, depth - 1)
    def leaf: Query = leaves(random.nextInt(leaves.size))
    if (depth == 0) leaf
    else
      random.nextInt(4) match {
        case 0 => Query.And(sub, sub)
        case 1 => Query.Or(sub, sub)
        case 2 => Query.Not(sub)
        case _ => leaf
      }
  }

  /**
    * Check that the query is rejected by the name and app restriction, not by some other
    * validation failure such as an unknown backend host or an unsupported operator.
    */
  private def assertRejectedAsUnrestricted(query: String): Unit = {
    val reason = "hi-res streams must restrict name and nf.app with :eq or :in"
    hiResRejection(query) match {
      case Some(msg) => assert(msg.contains(reason), s"unexpected rejection reason: $msg")
      case None      => fail(s"expected query to be rejected: $query")
    }
  }

  /** Previous implementation, kept as the reference for the equivalence check. */
  private def dnfIsRestricted(query: Query): Boolean = {
    def clause(q: Query, keys: Set[String]): Boolean = q match {
      case Query.And(q1, q2) => clause(q1, keys) || clause(q2, keys)
      case Query.Equal(k, _) => keys.contains(k)
      case Query.In(k, _)    => keys.contains(k)
      case _                 => false
    }
    def restricted(q: Query): Boolean =
      clause(q, Set("nf.app", "nf.cluster", "nf.asg")) && clause(q, Set("name"))
    Query.dnfList(query).forall(restricted)
  }

  test("hi-res query must restrict name and app") {
    assert(hiResIsValid("name,cpu,:eq,nf.app,www,:eq,:and"))
    assertRejectedAsUnrestricted("name,cpu,:eq")
    assertRejectedAsUnrestricted("nf.app,www,:eq")
  }

  test("hi-res query with or must restrict every branch") {
    assert(hiResIsValid("name,cpu,:eq,nf.app,www,:eq,:and,name,disk,:eq,nf.app,api,:eq,:and,:or"))
    assertRejectedAsUnrestricted("name,cpu,:eq,nf.app,www,:eq,:and,name,disk,:eq,:or")
  }

  test("hi-res check matches the dnf definition for generated queries") {
    val random = new scala.util.Random(42)
    val leaves = Vector[Query](
      Query.Equal("name", "cpu"),
      Query.Equal("nf.app", "www"),
      Query.Equal("nf.cluster", "www-main"),
      Query.Equal("other", "v"),
      Query.In("name", List("cpu", "disk")),
      Query.In("nf.app", List("www")),
      Query.HasKey("name"),
      Query.Regex("name", "c.*")
    )
    (0 until 2000).foreach { _ =>
      val q = genQuery(random, leaves, 4)
      assertEquals(
        hiResContext.isRestricted(q),
        dnfIsRestricted(q),
        s"disagreement for query: $q"
      )
    }
  }

  test("hi-res restriction check is not exponential in the number of or clauses") {
    // The disjunctive normal form for this query has 2^40 clauses, far more than could be
    // materialized. The structural check returns immediately.
    val leaf = (i: Int) => Query.Or(Query.Equal(s"k$i", "a"), Query.Equal(s"k$i", "b"))
    val ors = bigAnd(leaf)
    assert(!hiResContext.isRestricted(ors))

    // Both answers have to be produced structurally, otherwise an implementation that always
    // returned false would pass. Pinning name and nf.app makes the same query restricted.
    val pinned = Query.And(Query.Equal("name", "cpu"), Query.Equal("nf.app", "www"))
    assert(hiResContext.isRestricted(Query.And(pinned, ors)))
  }

  // The scoping check used to materialize the disjunctive normal form and expand the `:in`
  // clauses of every clause before looking at the keys. Both are exponential: the dnf in the
  // number of `:or` clauses, the expansion in the number of `:in` clauses. The tests below pin
  // the behavior and check it against that definition.

  /** `ignored-tag-keys` from the reference config, also used to build `hiResContext`. */
  private val ignoredTagKeys = ConfigFactory
    .load()
    .getStringList("atlas.eval.stream.ignored-tag-keys")
    .asScala
    .toSet

  /** Previous implementation, kept as the reference for the equivalence check. */
  private def dnfIsScoped(query: Query): Boolean = {
    Query
      .dnfList(query)
      .flatMap(q => Query.expandInClauses(q))
      .forall(q => (Query.exactKeys(q) -- ignoredTagKeys).nonEmpty)
  }

  test("scope check rejects data sources that are not scoped") {
    val step = java.time.Duration.ofMinutes(1)
    val reason = "narrow the scope to a specific app or name"
    assertEquals(rejection("name,cpu,:eq,:sum", step), None)
    assert(rejection("name,foo,:re,:sum", step).exists(_.contains(reason)))

    // An `:in` is only treated as an exact key if it is small enough to be expanded.
    assertEquals(rejection("nf.app,(,a,b,c,d,e,),:in,:sum", step), None)
    assert(rejection("nf.app,(,a,b,c,d,e,f,),:in,:sum", step).exists(_.contains(reason)))

    // Ignored keys do not scope a query.
    assert(ignoredTagKeys.contains("nf.region"))
    assert(rejection("nf.region,us-east-1,:eq,:sum", step).exists(_.contains(reason)))
  }

  test("scope check matches the dnf definition for generated queries") {
    val random = new scala.util.Random(42)
    val leaves = Vector[Query](
      Query.Equal("name", "cpu"),
      Query.Equal("nf.app", "www"),
      // Ignored keys, an exact match on one of these does not scope the query.
      Query.Equal("nf.region", "us-east-1"),
      Query.In("nf.account", List("1", "2")),
      Query.In("name", List("cpu", "disk")),
      Query.In("nf.app", List("a", "b", "c", "d", "e")),
      // Too many values to be expanded, so it does not contribute an exact key.
      Query.In("nf.app", List("a", "b", "c", "d", "e", "f")),
      Query.HasKey("name"),
      Query.Not(Query.Equal("name", "cpu")),
      Query.Regex("name", "c.*")
    )
    (0 until 2000).foreach { _ =>
      val q = genQuery(random, leaves, 3)
      assertEquals(hiResContext.isScoped(q), dnfIsScoped(q), s"disagreement for query: $q")
    }
  }

  test("scope check is not exponential in the number of or or in clauses") {
    // The disjunctive normal form for these queries has 2^40 clauses, and each clause of the
    // first would expand to 5^40 combinations. Neither could be materialized, the structural
    // check returns immediately.
    val ins = (i: Int) =>
      Query.Or(
        Query.In(s"k$i", List("a", "b", "c", "d", "e")),
        Query.In(s"j$i", List("a", "b", "c", "d", "e"))
      )
    assert(hiResContext.isScoped(bigAnd(ins)))

    // Both answers have to be produced structurally, otherwise an implementation that always
    // returned true would pass. Ignored keys do not scope the query.
    val ignored =
      (i: Int) => Query.Or(Query.Equal("nf.region", s"r$i"), Query.Equal("nf.account", s"a$i"))
    assert(!hiResContext.isScoped(bigAnd(ignored)))
  }

  /** Conjunction of 40 sub-queries, so the disjunctive normal form has at least 2^40 clauses. */
  private def bigAnd(f: Int => Query): Query = {
    (1 until 40).foldLeft(f(0))((acc, i) => Query.And(acc, f(i)))
  }
}
