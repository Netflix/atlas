/*
 * Copyright 2014-2018 Netflix, Inc.
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

import com.netflix.atlas.core.util.StringMatcher

sealed trait Query extends Expr {
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
}

object Query {

  /**
   * Return the set of keys explicitly referenced in the query. This can be useful for assisting
   * with automatic legends.
   */
  def exactKeys(query: Query): Set[String] = {
    query match {
      case And(q1, q2)  => exactKeys(q1) union exactKeys(q2)
      case Or(q1, q2)   => Set.empty
      case Not(q)       => Set.empty
      case Equal(k, _)  => Set(k)
      case q: KeyQuery  => Set.empty
      case _            => Set.empty
    }
  }

  /**
    * Extract a set of tags for the query based on the `:eq` clauses.
    */
  def tags(query: Query): Map[String, String] = {
    query match {
      case And(q1, q2)  => tags(q1) ++ tags(q2)
      case Or(q1, q2)   => Map.empty
      case Not(q)       => Map.empty
      case Equal(k, v)  => Map(k -> v)
      case q: KeyQuery  => Map.empty
      case _            => Map.empty
    }
  }

  /** Converts the input query into conjunctive normal form. */
  def cnf(query: Query): Query = {
    cnfList(query).reduceLeft { (q1, q2) => Query.And(q1, q2) }
  }

  /**
    * Converts the input query into a list of sub-queries that should be ANDd
    * together.
    */
  def cnfList(query: Query): List[Query] = {
    query match {
      case And(q1, q2)       => cnfList(q1) ::: cnfList(q2)
      case Or(q1, q2)        => crossOr(cnfList(q1), cnfList(q2))
      case Not(And(q1, q2))  => cnfList(Or(Not(q1), Not(q2)))
      case Not(Or(q1, q2))   => cnfList(Not(q1)) ::: cnfList(Not(q2))
      case Not(Not(q))       => List(q)
      case q                 => List(q)
    }
  }

  /** Converts the input query into disjunctive normal form. */
  def dnf(query: Query): Query = {
    dnfList(query).reduceLeft { (q1, q2) => Query.Or(q1, q2) }
  }

  /**
    * Converts the input query into a list of sub-queries that should be ORd
    * together.
    */
  def dnfList(query: Query): List[Query] = {
    query match {
      case And(q1, q2)       => crossAnd(dnfList(q1), dnfList(q2))
      case Or(q1, q2)        => dnfList(q1) ::: dnfList(q2)
      case Not(And(q1, q2))  => dnfList(Not(q1)) ::: dnfList(Not(q2))
      case Not(Or(q1, q2))   => dnfList(And(Not(q1), Not(q2)))
      case Not(Not(q))       => List(q)
      case q                 => List(q)
    }
  }

  private def crossOr(qs1: List[Query], qs2: List[Query]): List[Query] = {
    for (q1 <- qs1; q2 <- qs2) yield Or(q1, q2)
  }

  private def crossAnd(qs1: List[Query], qs2: List[Query]): List[Query] = {
    for (q1 <- qs1; q2 <- qs2) yield And(q1, q2)
  }

  case object True extends Query {
    def matches(tags: Map[String, String]): Boolean = true
    def matchesAny(tags: Map[String, List[String]]): Boolean = true
    def couldMatch(tags: Map[String, String]): Boolean = true
    def labelString: String = "true"
    override def toString: String = ":true"
    override def and(query: Query): Query = query
    override def or(query: Query): Query = Query.True
    override def not: Query = Query.False
  }

  case object False extends Query {
    def matches(tags: Map[String, String]): Boolean = false
    def matchesAny(tags: Map[String, List[String]]): Boolean = false
    def couldMatch(tags: Map[String, String]): Boolean = false
    def labelString: String = "false"
    override def toString: String = ":false"
    override def and(query: Query): Query = Query.False
    override def or(query: Query): Query = query
    override def not: Query = Query.True
  }

  sealed trait KeyQuery extends Query {
    def k: String
  }

  sealed trait KeyValueQuery extends KeyQuery {
    def check(s: String): Boolean
    def matches(tags: Map[String, String]): Boolean = tags.get(k).exists(check)
    def matchesAny(tags: Map[String, List[String]]): Boolean = tags.get(k).exists(_.exists(check))
    def couldMatch(tags: Map[String, String]): Boolean = tags.get(k).fold(true)(check)
  }

  case class HasKey(k: String) extends KeyQuery {
    def matches(tags: Map[String, String]): Boolean = tags.contains(k)
    def matchesAny(tags: Map[String, List[String]]): Boolean = tags.contains(k)
    def couldMatch(tags: Map[String, String]): Boolean = true
    def labelString: String = s"has($k)"
    override def toString: String = s"$k,:has"
  }

  case class Equal(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s == v
    def labelString: String = s"$k=$v"
    override def toString: String = s"$k,$v,:eq"
  }

  case class LessThan(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s < v
    def labelString: String = s"$k<$v"
    override def toString: String = s"$k,$v,:lt"
  }

  case class LessThanEqual(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s <= v
    def labelString: String = s"$k<=$v"
    override def toString: String = s"$k,$v,:le"
  }

  case class GreaterThan(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s > v
    def labelString: String = s"$k>$v"
    override def toString: String = s"$k,$v,:gt"
  }

  case class GreaterThanEqual(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s >= v
    def labelString: String = s"$k>=$v"
    override def toString: String = s"$k,$v,:ge"
  }

  sealed trait PatternQuery extends KeyValueQuery {
    def pattern: StringMatcher
  }

  case class Regex(k: String, v: String) extends PatternQuery {
    val pattern = StringMatcher.compile(s"^$v")
    def check(s: String): Boolean = pattern.matches(s)
    def labelString: String = s"$k~/^$v/"
    override def toString: String = s"$k,$v,:re"
  }

  case class RegexIgnoreCase(k: String, v: String) extends PatternQuery {
    val pattern = StringMatcher.compile(s"^$v", false)
    def check(s: String): Boolean = pattern.matches(s)
    def labelString: String = s"$k~/^$v/i"
    override def toString: String = s"$k,$v,:reic"
  }

  case class In(k: String, vs: List[String]) extends KeyValueQuery {
    private val values = vs.toSet
    def check(s: String): Boolean = values.contains(s)
    def labelString: String = s"$k in (${vs.mkString(",")})"
    override def toString: String = s"$k,(,${vs.mkString(",")},),:in"

    /** Convert this to a sequence of OR'd together equal queries. */
    def toOrQuery: Query = {
      if (vs.isEmpty) False else {
        vs.tail.foldLeft[Query](Equal(k, vs.head)) { case (acc, v) => Or(acc, Equal(k, v)) }
      }
    }
  }

  case class And(q1: Query, q2: Query) extends Query {
    def matches(tags: Map[String, String]): Boolean = q1.matches(tags) && q2.matches(tags)
    def matchesAny(tags: Map[String, List[String]]): Boolean = q1.matchesAny(tags) && q2.matchesAny(tags)
    def couldMatch(tags: Map[String, String]): Boolean = q1.couldMatch(tags) && q2.couldMatch(tags)
    def labelString: String = s"(${q1.labelString}) and (${q2.labelString})"
    override def toString: String = s"$q1,$q2,:and"
  }

  case class Or(q1: Query, q2: Query) extends Query {
    def matches(tags: Map[String, String]): Boolean = q1.matches(tags) || q2.matches(tags)
    def matchesAny(tags: Map[String, List[String]]): Boolean = q1.matchesAny(tags) || q2.matchesAny(tags)
    def couldMatch(tags: Map[String, String]): Boolean = q1.couldMatch(tags) || q2.couldMatch(tags)
    def labelString: String = s"(${q1.labelString}) or (${q2.labelString})"
    override def toString: String = s"$q1,$q2,:or"
  }

  case class Not(q: Query) extends Query {
    def matches(tags: Map[String, String]): Boolean = !q.matches(tags)
    def matchesAny(tags: Map[String, List[String]]): Boolean = !q.matchesAny(tags)
    def couldMatch(tags: Map[String, String]): Boolean = true
    def labelString: String = s"not(${q.labelString})"
    override def toString: String = s"$q,:not"
  }
}
