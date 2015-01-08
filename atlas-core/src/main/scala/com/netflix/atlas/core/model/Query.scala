/*
 * Copyright 2015 Netflix, Inc.
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
}

object Query {

  /**
   * Return the set of keys explicitly referenced in the query. This can be useful for assisting
   * with automatic legends.
   */
  def exactKeys(query: Query): Set[String] = {
    query match {
      case Query.And(q1, q2) => exactKeys(q1) union exactKeys(q2)
      case Query.Or(q1, q2)  => Set.empty
      case Query.Not(q)      => Set.empty
      case Query.Equal(k, _) => Set(k)
      case q: KeyQuery       => Set.empty
      case _                 => Set.empty
    }
  }

  case object True extends Query {
    def matches(tags: Map[String, String]): Boolean = true
    def matchesAny(tags: Map[String, List[String]]): Boolean = true
    def couldMatch(tags: Map[String, String]): Boolean = true
    override def toString: String = ":true"
  }

  case object False extends Query {
    def matches(tags: Map[String, String]): Boolean = false
    def matchesAny(tags: Map[String, List[String]]): Boolean = false
    def couldMatch(tags: Map[String, String]): Boolean = false
    override def toString: String = ":false"
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
    override def toString: String = s"$k,:has"
  }

  case class Equal(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s == v
    override def toString: String = s"$k,$v,:eq"
  }

  case class LessThan(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s < v
    override def toString: String = s"$k,$v,:lt"
  }

  case class LessThanEqual(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s <= v
    override def toString: String = s"$k,$v,:le"
  }

  case class GreaterThan(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s > v
    override def toString: String = s"$k,$v,:gt"
  }

  case class GreaterThanEqual(k: String, v: String) extends KeyValueQuery {
    def check(s: String): Boolean = s >= v
    override def toString: String = s"$k,$v,:ge"
  }

  sealed trait PatternQuery extends KeyValueQuery {
    def pattern: StringMatcher
  }

  case class Regex(k: String, v: String) extends PatternQuery {
    val pattern = StringMatcher.compile(s"^$v")
    def check(s: String): Boolean = pattern.matches(s)
    override def toString: String = s"$k,$v,:re"
  }

  case class RegexIgnoreCase(k: String, v: String) extends PatternQuery {
    val pattern = StringMatcher.compile(s"^$v", false)
    def check(s: String): Boolean = pattern.matches(s)
    override def toString: String = s"$k,$v,:reic"
  }

  case class In(k: String, vs: List[String]) extends KeyValueQuery {
    private val values = vs.toSet
    def check(s: String): Boolean = values.contains(s)
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
    override def toString: String = s"$q1,$q2,:and"
  }

  case class Or(q1: Query, q2: Query) extends Query {
    def matches(tags: Map[String, String]): Boolean = q1.matches(tags) || q2.matches(tags)
    def matchesAny(tags: Map[String, List[String]]): Boolean = q1.matchesAny(tags) || q2.matchesAny(tags)
    def couldMatch(tags: Map[String, String]): Boolean = q1.couldMatch(tags) || q2.couldMatch(tags)
    override def toString: String = s"$q1,$q2,:or"
  }

  case class Not(q: Query) extends Query {
    def matches(tags: Map[String, String]): Boolean = !q.matches(tags)
    def matchesAny(tags: Map[String, List[String]]): Boolean = !q.matchesAny(tags)
    def couldMatch(tags: Map[String, String]): Boolean = true
    override def toString: String = s"$q,:not"
  }
}
