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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.impl.PatternMatcher

sealed trait Query extends Expr {

  /** Returns true if the query expression matches the tags provided by the function. */
  def matches(tags: String => String): Boolean

  /** Returns true if the query expression matches the tags. */
  def matches(tags: Map[String, String]): Boolean

  /** Returns true if the query matches for any of the items in the list. */
  def matchesAny(tags: Map[String, List[String]]): Boolean

  /**
    * Returns true if the query expression could match if additional tags were added. Typically
    * used for doing some initial filtering based on a partial list of common tags.
    */
  def couldMatch(tags: Map[String, String]): Boolean

  /** Returns a string that summarizes the query expression in a human readable format. */
  def labelString: String

  def and(query: Query): Query = query match {
    case Query.True  => this
    case Query.False => Query.False
    case q           => Query.And(this, q)
  }

  def or(query: Query): Query = query match {
    case Query.True  => Query.True
    case Query.False => this
    case q           => Query.Or(this, q)
  }

  def not: Query = Query.Not(this)

  /**
    * Hash code is cached to allow cheaper lookup during evaluation. This implementation
    * in the base interface depends on the main fields of the case class being set prior
    * to `super()` being called in the case class constructor. That appears to be the case
    * with current scala versions.
    */
  override val hashCode: Int = scala.util.hashing.MurmurHash3.productHash(this)
}

object Query {

  /**
    * Return the set of keys explicitly referenced in the query. This can be useful for assisting
    * with automatic legends.
    */
  def exactKeys(query: Query): Set[String] = {
    query match {
      case And(q1, q2) => exactKeys(q1).union(exactKeys(q2))
      case Or(_, _)    => Set.empty
      case Not(_)      => Set.empty
      case Equal(k, _) => Set(k)
      case _: KeyQuery => Set.empty
      case _           => Set.empty
    }
  }

  /**
    * Return the set of keys referenced by in the query.
    */
  def allKeys(query: Query): Set[String] = {
    query match {
      case And(q1, q2) => allKeys(q1) ++ allKeys(q2)
      case Or(q1, q2)  => allKeys(q1) ++ allKeys(q2)
      case Not(q)      => allKeys(q)
      case q: KeyQuery => Set(q.k)
      case _           => Set.empty
    }
  }

  /**
    * Extract a set of tags for the query based on the `:eq` clauses.
    */
  def tags(query: Query): Map[String, String] = {
    query match {
      case And(q1, q2) => tags(q1) ++ tags(q2)
      case Or(_, _)    => Map.empty
      case Not(_)      => Map.empty
      case Equal(k, v) => Map(k -> v)
      case _: KeyQuery => Map.empty
      case _           => Map.empty
    }
  }

  /** Converts the input query into conjunctive normal form. */
  def cnf(query: Query): Query = {
    cnfList(query).reduceLeft { (q1, q2) =>
      Query.And(q1, q2)
    }
  }

  /**
    * Converts the input query into a list of sub-queries that should be ANDd
    * together.
    */
  def cnfList(query: Query): List[Query] = {
    query match {
      case And(q1, q2)      => cnfList(q1) ::: cnfList(q2)
      case Or(q1, q2)       => crossOr(cnfList(q1), cnfList(q2))
      case Not(And(q1, q2)) => cnfList(Or(Not(q1), Not(q2)))
      case Not(Or(q1, q2))  => cnfList(Not(q1)) ::: cnfList(Not(q2))
      case Not(Not(q))      => List(q)
      case q                => List(q)
    }
  }

  /** Converts the input query into disjunctive normal form. */
  def dnf(query: Query): Query = {
    dnfList(query).reduceLeft { (q1, q2) =>
      Query.Or(q1, q2)
    }
  }

  /**
    * Converts the input query into a list of sub-queries that should be ORd
    * together.
    */
  def dnfList(query: Query): List[Query] = {
    query match {
      case And(q1, q2)      => crossAnd(dnfList(q1), dnfList(q2))
      case Or(q1, q2)       => dnfList(q1) ::: dnfList(q2)
      case Not(And(q1, q2)) => dnfList(Not(q1)) ::: dnfList(Not(q2))
      case Not(Or(q1, q2))  => dnfList(And(Not(q1), Not(q2)))
      case Not(Not(q))      => List(q)
      case q                => List(q)
    }
  }

  private def crossOr(qs1: List[Query], qs2: List[Query]): List[Query] = {
    for (q1 <- qs1; q2 <- qs2) yield Or(q1, q2)
  }

  private def crossAnd(qs1: List[Query], qs2: List[Query]): List[Query] = {
    for (q1 <- qs1; q2 <- qs2) yield And(q1, q2)
  }

  /**
    * Split :in queries into a list of queries using :eq. The query should be normalized ahead
    * of time so it is a string of conjunctions. See `dnfList` for more information.
    *
    * In order to avoid a massive combinatorial explosion clauses that have more than `limit`
    * expressions will not be expanded. The default limit is 5. It is somewhat arbitrary, but
    * seems to work well in practice for the current query data sets at Netflix.
    */
  def expandInClauses(query: Query, limit: Int = 5): List[Query] = {
    query match {
      case Query.And(q1, q2) =>
        for {
          a <- expandInClauses(q1, limit)
          b <- expandInClauses(q2, limit)
        } yield {
          Query.And(a, b)
        }
      case Query.In(k, vs) if vs.lengthCompare(limit) <= 0 =>
        vs.map { v =>
          Query.Equal(k, v)
        }
      case _ => List(query)
    }
  }

  /**
    * Simplify a query expression that contains True and False constants.
    *
    * @param query
    *     Query expression to simplify.
    * @param ignore
    *     If true, then the simplification is for the purposes of ignoring certain
    *     query terms. This comes up in some automatic rewriting use-cases where
    *     we need to have a restriction clause based on a subset of the keys and
    *     ignore the rest. Consider the following example:
    *
    *     ```
    *     nf.app,www,:eq,name,http.requests,:eq,:and,status,200,:eq,:not,:and,:sum
    *     ```
    *
    *     Lets suppose I want to simplify this to a base query for the common tags
    *     that could be reused with other metrics. One way would be to rewrite all
    *     key query clauses that are not using common tags (prefixed with nf.) to
    *     `Query.True` and simplify. However the not clause causes a problem:
    *
    *     ```
    *              rewrite - nf.app,www,:eq,:true,:and,:true,:not,:and
    *     simplification 1 - nf.app,www.:eq,:false,:and
    *     simplification 2 - :false
    *     ```
    *
    *     The ignore mode will cause `:true,:not` to map to `:true` so the term will
    *     get ignored rather than reduce the entire expression to `:false`. Default
    *     value is false.
    * @return
    *     The simplified query expression.
    */
  def simplify(query: Query, ignore: Boolean = false): Query = {
    val newQuery = query match {
      case Query.And(Query.True, q)  => simplify(q, ignore)
      case Query.And(q, Query.True)  => simplify(q, ignore)
      case Query.And(Query.False, _) => Query.False
      case Query.And(_, Query.False) => Query.False
      case Query.And(q1, q2)         => Query.And(simplify(q1, ignore), simplify(q2, ignore))

      case Query.Or(Query.True, _)  => Query.True
      case Query.Or(_, Query.True)  => Query.True
      case Query.Or(Query.False, q) => simplify(q, ignore)
      case Query.Or(q, Query.False) => simplify(q, ignore)
      case Query.Or(q1, q2)         => Query.Or(simplify(q1, ignore), simplify(q2, ignore))

      case Query.Not(Query.True)  => if (ignore) Query.True else Query.False
      case Query.Not(Query.False) => Query.True
      case Query.Not(q)           => Query.Not(simplify(q, ignore))

      case q => q
    }

    if (newQuery != query) simplify(newQuery, ignore) else newQuery
  }

  case object True extends Query {

    def matches(tags: String => String): Boolean = true

    def matches(tags: Map[String, String]): Boolean = true

    def matchesAny(tags: Map[String, List[String]]): Boolean = true

    def couldMatch(tags: Map[String, String]): Boolean = true

    def labelString: String = "true"

    override def append(builder: java.lang.StringBuilder): Unit = {
      builder.append(":true")
    }

    override def and(query: Query): Query = query

    override def or(query: Query): Query = Query.True

    override def not: Query = Query.False
  }

  case object False extends Query {

    def matches(tags: String => String): Boolean = false

    def matches(tags: Map[String, String]): Boolean = false

    def matchesAny(tags: Map[String, List[String]]): Boolean = false

    def couldMatch(tags: Map[String, String]): Boolean = false

    def labelString: String = "false"

    override def append(builder: java.lang.StringBuilder): Unit = {
      builder.append(":false")
    }

    override def and(query: Query): Query = Query.False

    override def or(query: Query): Query = query

    override def not: Query = Query.True
  }

  sealed trait KeyQuery extends Query {

    def k: String
  }

  sealed trait KeyValueQuery extends KeyQuery {

    def check(s: String): Boolean

    def matches(tags: String => String): Boolean = {
      val v = tags(k)
      v != null && check(v)
    }

    def matches(tags: Map[String, String]): Boolean = {
      tags match {
        case ts: SortedTagMap =>
          val v = ts.getOrNull(k)
          v != null && check(v)
        case _ =>
          tags.get(k).exists(check)
      }
    }

    def matchesAny(tags: Map[String, List[String]]): Boolean = {
      tags.get(k).exists(_.exists(check))
    }

    def couldMatch(tags: Map[String, String]): Boolean = {
      tags match {
        case ts: SortedTagMap =>
          val v = ts.getOrNull(k)
          v == null || check(v)
        case _ =>
          tags.get(k).fold(true)(check)
      }
    }
  }

  case class HasKey(k: String) extends KeyQuery {

    def matches(tags: String => String): Boolean = tags(k) != null

    def matches(tags: Map[String, String]): Boolean = tags.contains(k)

    def matchesAny(tags: Map[String, List[String]]): Boolean = tags.contains(k)

    def couldMatch(tags: Map[String, String]): Boolean = true

    def labelString: String = s"has($k)"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, Interpreter.WordToken(":has"))
    }
  }

  case class Equal(k: String, v: String) extends KeyValueQuery {

    def check(s: String): Boolean = s == v

    def labelString: String = s"$k=$v"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, v, Interpreter.WordToken(":eq"))
    }
  }

  case class LessThan(k: String, v: String) extends KeyValueQuery {

    def check(s: String): Boolean = s < v

    def labelString: String = s"$k<$v"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, v, Interpreter.WordToken(":lt"))
    }
  }

  case class LessThanEqual(k: String, v: String) extends KeyValueQuery {

    def check(s: String): Boolean = s <= v

    def labelString: String = s"$k<=$v"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, v, Interpreter.WordToken(":le"))
    }
  }

  case class GreaterThan(k: String, v: String) extends KeyValueQuery {

    def check(s: String): Boolean = s > v

    def labelString: String = s"$k>$v"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, v, Interpreter.WordToken(":gt"))
    }
  }

  case class GreaterThanEqual(k: String, v: String) extends KeyValueQuery {

    def check(s: String): Boolean = s >= v

    def labelString: String = s"$k>=$v"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, v, Interpreter.WordToken(":ge"))
    }
  }

  sealed trait PatternQuery extends KeyValueQuery {

    def pattern: PatternMatcher
  }

  case class Regex(k: String, v: String) extends PatternQuery {

    val pattern: PatternMatcher = PatternMatcher.compile(s"^$v")

    def check(s: String): Boolean = pattern.matches(s)

    def labelString: String = s"$k~/^$v/"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, v, Interpreter.WordToken(":re"))
    }
  }

  case class RegexIgnoreCase(k: String, v: String) extends PatternQuery {

    val pattern: PatternMatcher = PatternMatcher.compile(s"^$v").ignoreCase()

    def check(s: String): Boolean = pattern.matches(s)

    def labelString: String = s"$k~/^$v/i"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, v, Interpreter.WordToken(":reic"))
    }
  }

  case class In(k: String, vs: List[String]) extends KeyValueQuery {

    private val values = vs.toSet

    def check(s: String): Boolean = values.contains(s)

    def labelString: String = s"$k in (${vs.mkString(",")})"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, k, vs, Interpreter.WordToken(":in"))
    }

    /** Convert this to a sequence of OR'd together equal queries. */
    def toOrQuery: Query = {
      if (vs.isEmpty) False
      else {
        vs.tail.foldLeft[Query](Equal(k, vs.head)) { case (acc, v) => Or(acc, Equal(k, v)) }
      }
    }
  }

  case class And(q1: Query, q2: Query) extends Query {

    def matches(tags: String => String): Boolean = q1.matches(tags) && q2.matches(tags)

    def matches(tags: Map[String, String]): Boolean = q1.matches(tags) && q2.matches(tags)

    def matchesAny(tags: Map[String, List[String]]): Boolean =
      q1.matchesAny(tags) && q2.matchesAny(tags)

    def couldMatch(tags: Map[String, String]): Boolean = q1.couldMatch(tags) && q2.couldMatch(tags)

    def labelString: String = s"(${q1.labelString}) and (${q2.labelString})"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q1, q2, Interpreter.WordToken(":and"))
    }
  }

  case class Or(q1: Query, q2: Query) extends Query {

    def matches(tags: String => String): Boolean = q1.matches(tags) || q2.matches(tags)

    def matches(tags: Map[String, String]): Boolean = q1.matches(tags) || q2.matches(tags)

    def matchesAny(tags: Map[String, List[String]]): Boolean =
      q1.matchesAny(tags) || q2.matchesAny(tags)

    def couldMatch(tags: Map[String, String]): Boolean = q1.couldMatch(tags) || q2.couldMatch(tags)

    def labelString: String = s"(${q1.labelString}) or (${q2.labelString})"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q1, q2, Interpreter.WordToken(":or"))
    }
  }

  case class Not(q: Query) extends Query {

    def matches(tags: String => String): Boolean = !q.matches(tags)

    def matches(tags: Map[String, String]): Boolean = !q.matches(tags)

    def matchesAny(tags: Map[String, List[String]]): Boolean = !q.matchesAny(tags)

    def couldMatch(tags: Map[String, String]): Boolean = !q.matches(tags)

    def labelString: String = s"not(${q.labelString})"

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, q, Interpreter.WordToken(":not"))
    }
  }
}
