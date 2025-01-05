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

import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.typesafe.config.Config

/**
  * Vocabulary that allows custom extension operations to be loaded from the
  * config.
  *
  * @param config
  *     Config instance to use for loading the custom operations. The settings
  *     will be loaded from the `atlas.core.vocabulary` block.
  *
  *     **Words**
  *
  *     Custom words can be defined using an expression. These are typically used
  *     by the operators to provide common helper functions.
  *
  *     ```
  *     words = [
  *       {
  *         name = "square"
  *         body = ":dup,:mul"
  *         examples = ["2"]
  *       }
  *     ]
  *     ```
  *
  *     The supported fields are:
  *
  *     - `name`: operation name, when the user calls the operation they will use
  *       `:\$name`.
  *     - `body`: expression that is executed for this operation.
  *     - `examples`: set of example stacks that can be used as input to the operator
  *       to show how it works.
  *
  *     **Averages**
  *
  *     The `custom-averages` list contains a set of rewrites for averaging based
  *     on an arbitrary denominator query. This is typically used to make it easier
  *     for performing an average based on a separate infrastructure metric. For
  *     example, at Netflix there is a metric published for each instance that is
  *     UP in Eureka. To compute an average per UP server we could define a custom
  *     average like:
  *
  *     ```
  *     custom-averages = [
  *       {
  *         name = "eureka-avg"
  *         base-query = "name,eureka.state,:eq,status,UP,:eq,:and"
  *         keys = ["nf.app", "nf.cluster", "nf.asg", "nf.node"]
  *       }
  *     ]
  *     ```
  *
  *     The supported fields are:
  *
  *     - `name`: operation name, when the user calls the operation they will use
  *       `:\$name`.
  *     - `base-query`: query for the denominator.
  *     - `keys`: tag keys that are available for use on the denominator.
  *
  * @param dependencies
  *     Other vocabularies to depend on, defaults to the `StyleVocabulary`.
  */
class CustomVocabulary(config: Config, dependencies: List[Vocabulary] = List(StyleVocabulary))
    extends Vocabulary {

  import CustomVocabulary.*
  import scala.jdk.CollectionConverters.*

  val name: String = "custom"

  val dependsOn: List[Vocabulary] = dependencies

  val words: List[Word] = {
    val vocab = config.getConfig("atlas.core.vocabulary")
    val macros = loadCustomWords(vocab.getConfigList("words").asScala.toList)
    val averages = loadCustomAverages(vocab.getConfigList("custom-averages").asScala.toList)
    macros ::: averages
  }

  private def loadCustomWords(configs: List[Config]): List[Word] = {
    configs.map { cfg =>
      val name = cfg.getString("name")
      val body = Interpreter.splitAndTrim(cfg.getString("body"))
      val examples = cfg.getStringList("examples").asScala.toList
      StandardVocabulary.Macro(name, body, examples)
    }
  }

  private def loadCustomAverages(configs: List[Config]): List[Word] = {
    configs.map { cfg =>
      val name = cfg.getString("name")
      val baseQuery = eval(cfg.getString("base-query"))
      val keys = cfg.getStringList("keys").asScala.toSet
      CustomAvg(name, baseQuery, keys)
    }
  }
}

object CustomVocabulary {

  private val queryInterpreter = Interpreter(QueryVocabulary.allWords)

  private def eval(s: String): Query = {
    queryInterpreter.execute(s).stack match {
      case (q: Query) :: Nil => q
      case _                 => throw new IllegalArgumentException(s)
    }
  }

  case class CustomAvg(name: String, baseQuery: Query, keys: Set[String])
      extends Word
      with Function2[Expr, List[String], Expr] {

    def matches(stack: List[Any]): Boolean = {
      if (matcher.isDefinedAt(stack)) matcher(stack) else false
    }

    def execute(context: Context): Context = {
      val pf = executor(context)
      if (pf.isDefinedAt(context.stack))
        context.copy(stack = pf(context.stack))
      else
        invalidStack
    }

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: _ => true
    }

    protected def executor(context: Context): PartialFunction[List[Any], List[Any]] = {
      case (q: Query) :: s =>
        val nq = extractCommonQuery(q)
        val numerator = DataExpr.Sum(q)
        val denominator = DataExpr.Sum(baseQuery.and(nq))
        val avg = MathExpr.Divide(numerator, denominator)
        val ctxt = Context(context.interpreter, Nil, Map.empty)
        val rewrite = Some(this)
        MathExpr.NamedRewrite(name, q, Nil, avg, ctxt, rewrite) :: s
    }

    /**
      * Function used for the group by rewrite, see MathExpr.NamedRewrite for more details. It
      * is done this way to allow the equality check to work correctly as it will be based on
      * this word rather than a generated function class.
      */
    def apply(expr: Expr, ks: List[String]): Expr = {
      val q = expr.asInstanceOf[Query]
      val nq = extractCommonQuery(q)
      val numerator = DataExpr.Sum(q)
      val denominator = DataExpr.Sum(baseQuery.and(nq))
      val denominatorKeys = ks.filter(keys.contains)
      if (denominatorKeys.isEmpty) {
        MathExpr.Divide(DataExpr.GroupBy(numerator, ks), denominator)
      } else {
        MathExpr.Divide(
          DataExpr.GroupBy(numerator, ks),
          DataExpr.GroupBy(denominator, denominatorKeys)
        )
      }
    }

    /**
      * Extract the portions of the user query that are also applicable to the
      * denominator. This will be used to restrict the scope of `baseQuery` so
      * that it will match that of the user query.
      *
      * As an example, suppose we want to compute an average per node for requests
      * that have a 404 status code. The user query would be something like:
      *
      * ```
      * name,http.requests,:eq,status,404,:eq,:and,nf.app,www,:eq,:and
      * ```
      *
      * If we have a separate metric that indicates the number of instances and is
      * only tagged with `nf.app`, then the common query we need to extract is:
      *
      * ```
      * nf.app,www,:eq
      * ```
      */
    private[model] def extractCommonQuery(query: Query): Query = {
      val tmp = query.rewrite {
        case kq: Query.KeyQuery if !keys.contains(kq.k) => Query.True
      }
      Query.simplify(tmp.asInstanceOf[Query], ignore = true)
    }

    override def summary: String =
      s"""
         |Compute the average using `$baseQuery` as the denominator. The following
         |keys can be used to restrict the scope or as part of the group by:
         |
         |${keys.mkString("- ", "\n   |- ", "")}
      """.stripMargin.trim

    override def signature: String = "Query -- TimeSeriesExpr"

    override def examples: List[String] = Nil
  }
}
