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

  case object True extends SimpleWord {
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

  case object False extends SimpleWord {
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

  case object HasKey extends SimpleWord {
    override def name: String = "has"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (k: String) :: s => Query.HasKey(k) :: s
    }

    override def summary: String =
      """
        |Query expression that matches time series that have a key with the specified name.
        |Suppose you have three time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `status,:has` would match:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |
        |The query `type,:has` would match:
        |
        |* `name=sys.cpu, type=user, nf.app=server`
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

  case object Equal extends KeyValueWord {
    override def name: String = "eq"

    def newInstance(k: String, v: String): Query = Query.Equal(k, v)

    override def summary: String =
      """
        |Query expression that matches time series where the value for a given key is an exact
        |match for the provided value. Suppose you have three time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `name,http.requests,:eq` would be equivalent to an infix query like
        |`name = http.requests` and would match:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
      """.stripMargin.trim
  }

  case object LessThan extends KeyValueWord {
    override def name: String = "lt"

    def newInstance(k: String, v: String): Query = Query.LessThan(k, v)

    override def summary: String =
      """
        |Query expression that matches time series where the value for a given key is less than
        |the provided value. Suppose you have three time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `status,400,:lt` would be equivalent to an infix query like
        |`status < 400` and would match:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |
        |Note that all tag values are strings and will be compared lexically not numerically. So
        |for example 100 would be less than 2.
      """.stripMargin.trim
  }

  case object LessThanEqual extends KeyValueWord {
    override def name: String = "le"

    def newInstance(k: String, v: String): Query = Query.LessThanEqual(k, v)

    override def summary: String =
      """
        |Query expression that matches time series where the value for a given key is less than
        |or equal to the provided value. Suppose you have four time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=202, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `status,202,:le` would be equivalent to an infix query like
        |`status <= 202` and would match:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=202, nf.app=server`
        |
        |Note that all tag values are strings and will be compared lexically not numerically. So
        |for example 100 would be less than 2.
      """.stripMargin.trim
  }

  case object GreaterThan extends KeyValueWord {
    override def name: String = "gt"

    def newInstance(k: String, v: String): Query = Query.GreaterThan(k, v)

    override def summary: String =
      """
        |Query expression that matches time series where the value for a given key is greater than
        |the provided value. Suppose you have three time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `status,399,:gt` would be equivalent to an infix query like
        |`status > 399` and would match:
        |
        |* `name=http.requests, status=400, nf.app=server`
        |
        |Note that all tag values are strings and will be compared lexically not numerically. So
        |for example 2 would be greater than 100.
      """.stripMargin.trim
  }

  case object GreaterThanEqual extends KeyValueWord {
    override def name: String = "ge"

    def newInstance(k: String, v: String): Query = Query.GreaterThanEqual(k, v)

    override def summary: String =
      """
        |Query expression that matches time series where the value for a given key is greater than
        |or equal to the provided value. Suppose you have three time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `status,400,:ge` would be equivalent to an infix query like
        |`status >= 400` and would match:
        |
        |* `name=http.requests, status=400, nf.app=server`
        |
        |Note that all tag values are strings and will be compared lexically not numerically. So
        |for example 2 would be greater than 100.
      """.stripMargin.trim
  }

  case object Regex extends KeyValueWord {
    override def name: String = "re"

    def newInstance(k: String, v: String): Query = Query.Regex(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with a value that matches the provided regular
        |expression.
        |
        |> :warning: Regular expressions without a clear prefix force a full scan and should be
        |avoided.
        |
        |See the [java regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html)
        |docs for more information about supported patterns. The regex will be anchored to the start
        |and should have a clear prefix.
        |
        |Suppose you have four time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=http.requests, status=400, nf.app=server`
        |* `name=http.requests, status=404, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `status,4(?!04),:re` would match 4xx status codes other than 404:
        |
        |* `name=http.requests, status=400, nf.app=server`
      """.stripMargin.trim

    override def examples: List[String] = List(
      "name,DiscoveryStatus_(UP|DOWN)",
      "name,discoverystatus_(Up|Down)",
      "ERROR:name")
  }

  case object RegexIgnoreCase extends KeyValueWord {
    override def name: String = "reic"

    def newInstance(k: String, v: String): Query = Query.RegexIgnoreCase(k, v)

    override def summary: String =
      """
        |Query expression that matches time series with a value that matches the provided regular
        |expression with case insensitive matching enabled.
        |
        |> :warning: This operation always requires a full scan and should be avoided if at all
        |possible. Queries using this operation may be de-priortized.
        |
        |See the [java regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html)
        |
        |Suppose you have three time series:
        |
        |* `name=http.numRequests, status=200, nf.app=server`
        |* `name=http.numrequests, status=400, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=server`
        |
        |The query `name,http.numrequests,:reic` would match:
        |
        |* `name=http.numRequests, status=200, nf.app=server`
        |* `name=http.numrequests, status=400, nf.app=server`
      """.stripMargin.trim

    override def examples: List[String] = List(
      "name,DiscoveryStatus_(UP|DOWN)",
      "name,discoverystatus_(Up|Down)",
      "ERROR:name")
  }

  case object In extends SimpleWord {
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
        |Query expression that matches time series where the value for a given key is in the
        |provided set. Suppose you have three time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=foo`
        |* `name=sys.cpu, type=user, nf.app=bar`
        |
        |The query `nf.app,(,foo,bar,),:in` would match:
        |
        |* `name=sys.cpu, type=user, nf.app=foo`
        |* `name=sys.cpu, type=user, nf.app=bar`
      """.stripMargin.trim

    override def signature: String = "k:String vs:List -- Query"

    override def examples: List[String] = List(
      "name,(,sps,)",
      "name,(,requestsPerSecond,sps,)",
      "name,(,)",
      "ERROR:name,sps")
  }

  case object And extends SimpleWord {
    override def name: String = "and"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: (_: Query) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (q2: Query) :: (q1: Query) :: s => (q1 and q2) :: s
    }

    override def summary: String =
      """
        |Query expression that matches if both sub queries match. Suppose you have three time
        |series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=foo`
        |* `name=sys.cpu, type=user, nf.app=bar`
        |
        |The query `name,sys.cpu,:eq,nf.app,foo,:eq,:and` would match:
        |
        |* `name=sys.cpu, type=user, nf.app=foo`
      """.stripMargin.trim

    override def signature: String = "Query Query -- Query"

    override def examples: List[String] =
      List(":false,:false", ":false,:true", ":true,:false", ":true,:true")
  }

  case object Or extends SimpleWord {
    override def name: String = "or"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: (_: Query) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (q2: Query) :: (q1: Query) :: s => (q1 or q2) :: s
    }

    override def summary: String =
      """
        |Query expression that matches if either sub query matches. Suppose you have three time
        |series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=foo`
        |* `name=sys.cpu, type=user, nf.app=bar`
        |
        |The query `nf.app,foo,:eq,nf.app,bar,:eq,:or` would match:
        |
        |* `name=sys.cpu, type=user, nf.app=foo`
        |* `name=sys.cpu, type=user, nf.app=bar`
        |
        |Note for the example above where both sides are for the same key, [:in](query-in) is a
        |better option.
      """.stripMargin.trim

    override def signature: String = "Query Query -- Query"

    override def examples: List[String] =
      List(":false,:false", ":false,:true", ":true,:false", ":true,:true")
  }

  case object Not extends SimpleWord {
    override def name: String = "not"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (q: Query) :: s  => q.not :: s
    }

    override def summary: String =
      """
        |Query expression that matches if the sub query does not matches.
        |Suppose you have three time series:
        |
        |* `name=http.requests, status=200, nf.app=server`
        |* `name=sys.cpu, type=user, nf.app=foo`
        |* `name=sys.cpu, type=user, nf.app=bar`
        |
        |The query `name,sys.cpu,:eq,:not` would match:
        |
        |* `name=http.requests, status=200, nf.app=server`
      """.stripMargin.trim

    override def signature: String = "Query -- Query"

    override def examples: List[String] = List(":false", ":true")
  }

}
