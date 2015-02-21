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

import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word


object QueryVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.ModelExtractors._

  val name: String = "query"

  val dependsOn: List[Vocabulary] = List(StandardVocabulary)

  val words: List[Word] = List(
    True,
    False,
    HasKey,
    Equal,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    Regex,
    RegexIgnoreCase,
    In,
    And,
    Or,
    Not
  )

  object True extends SimpleWord {
    override def name: String = "true"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case _ => true }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case s => Query.True :: s
    }

    override def summary: String =
      """
        |Query expression that matches all input time series.
      """.stripMargin.trim

    override def signature: String = " -- Query"

    override def examples: List[String] = List("")
  }

  object False extends SimpleWord {
    override def name: String = "false"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case _ => true }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case s => Query.False :: s
    }

    override def summary: String =
      """
        |Query expression that will not match any input time series.
      """.stripMargin.trim

    override def signature: String = " -- Query"

    override def examples: List[String] = List("")
  }

  object HasKey extends SimpleWord {
    override def name: String = "has"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (k: String) :: s => Query.HasKey(k) :: s
    }

    override def summary: String =
      """
        |Query expression that matches time series with `tags.contains(k)`.
      """.stripMargin.trim

    override def signature: String = "k:String -- Query"

    override def examples: List[String] = List("a", "name", "ERROR:")
  }

  trait KeyValueWord extends SimpleWord {
    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: (_: String) :: _ => true
    }

    def newInstance(k: String, v: String): Query

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (v: String) :: (k: String) :: s => newInstance(k, v) :: s
    }

    override def signature: String = "k:String v:String -- Query"

    override def examples: List[String] = List(
      "a,b",
      "nf.node,silverlight-003e",
      "ERROR:name")
  }

  object Equal extends KeyValueWord {
    override def name: String = "eq"

    def newInstance(k: String, v: String): Query = Query.Equal(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with `tags[k] == v`.
      """.stripMargin.trim
  }

  object LessThan extends KeyValueWord {
    override def name: String = "lt"

    def newInstance(k: String, v: String): Query = Query.LessThan(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with `tags[k] < v`.
      """.stripMargin.trim
  }

  object LessThanEqual extends KeyValueWord {
    override def name: String = "le"

    def newInstance(k: String, v: String): Query = Query.LessThanEqual(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with `tags[k] <= v`.
      """.stripMargin.trim
  }

  object GreaterThan extends KeyValueWord {
    override def name: String = "gt"

    def newInstance(k: String, v: String): Query = Query.GreaterThan(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with `tags[k] > v`.
      """.stripMargin.trim
  }

  object GreaterThanEqual extends KeyValueWord {
    override def name: String = "ge"

    def newInstance(k: String, v: String): Query = Query.GreaterThanEqual(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with `tags[k] >= v`.
      """.stripMargin.trim
  }

  object Regex extends KeyValueWord {
    override def name: String = "re"

    def newInstance(k: String, v: String): Query = Query.Regex(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with `tags[k] =~ /^v/`.
        |
        |> :warning: Regular expressions without a clear prefix force a full scan and should be
        |avoided.
      """.stripMargin.trim

    override def examples: List[String] = List(
      "name,DiscoveryStatus_(UP|DOWN)",
      "name,discoverystatus_(Up|Down)",
      "ERROR:name")
  }

  object RegexIgnoreCase extends KeyValueWord {
    override def name: String = "reic"

    def newInstance(k: String, v: String): Query = Query.RegexIgnoreCase(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with `tags[k] =~ /^v/i`.
        |
        |> :warning: This operation requires a full scan and should be avoided it at all
        |possible.
      """.stripMargin.trim

    override def examples: List[String] = List(
      "name,DiscoveryStatus_(UP|DOWN)",
      "name,discoverystatus_(Up|Down)",
      "ERROR:name")
  }

  object In extends SimpleWord {
    override def name: String = "in"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: List[_]) :: (_: String) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case Nil :: (k: String) :: s                  => Query.False :: s
      case ((v: String) :: Nil) :: (k: String) :: s => Query.Equal(k, v) :: s
      case StringListType(vs) :: (k: String) :: s   => Query.In(k, vs) :: s
    }

    override def summary: String =
      """
        |Query expression that matches time series with `vs.contains(tags[k])`.
      """.stripMargin.trim

    override def signature: String = "k:String vs:List -- Query"

    override def examples: List[String] = List(
      "name,(,sps,)",
      "name,(,requestsPerSecond,sps,)",
      "name,(,)",
      "ERROR:name,sps")
  }

  object And extends SimpleWord {
    override def name: String = "and"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: (_: Query) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (_: Query) :: Query.False :: s  => Query.False :: s
      case Query.False :: (_: Query) :: s  => Query.False :: s
      case (q: Query) :: Query.True :: s   => q :: s
      case Query.True :: (q: Query) :: s   => q :: s
      case (q2: Query) :: (q1: Query) :: s => Query.And(q1, q2) :: s
    }

    override def summary: String =
      """
        |Query expression that will match iff both sub queries match.
      """.stripMargin.trim

    override def signature: String = "Query Query -- Query"

    override def examples: List[String] =
      List(":false,:false", ":false,:true", ":true,:false", ":true,:true")
  }

  object Or extends SimpleWord {
    override def name: String = "or"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: (_: Query) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (q: Query) :: Query.False :: s  => q :: s
      case Query.False :: (q: Query) :: s  => q :: s
      case (_: Query) :: Query.True :: s   => Query.True :: s
      case Query.True :: (_: Query) :: s   => Query.True :: s
      case (q2: Query) :: (q1: Query) :: s => Query.Or(q1, q2) :: s
    }

    override def summary: String =
      """
        |Query expression that will match if either sub query matches.
      """.stripMargin.trim

    override def signature: String = "Query Query -- Query"

    override def examples: List[String] =
      List(":false,:false", ":false,:true", ":true,:false", ":true,:true")
  }

  object Not extends SimpleWord {
    override def name: String = "not"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case Query.False :: s => Query.True :: s
      case Query.True :: s  => Query.False :: s
      case (q: Query) :: s  => Query.Not(q) :: s
    }

    override def summary: String =
      """
        |Query expression that will match if the sub query doesn't match.
      """.stripMargin.trim

    override def signature: String = "Query -- Query"

    override def examples: List[String] = List(":false", ":true")
  }

}
