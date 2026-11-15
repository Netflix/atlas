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
package com.netflix.atlas.core.model

import scala.collection.immutable.ArraySeq

import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.stacklang.TypedMacro
import com.netflix.atlas.core.stacklang.TypedWord
import com.netflix.atlas.core.stacklang.UnknownWordException
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.stacklang.ast.DataType
import com.netflix.atlas.core.stacklang.ast.Parameter
import com.netflix.atlas.core.util.Features
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
  *     - `summary`: optional description of the operator. Only used for typed words, if not
  *       specified, then it will show the body.
  *     - `parameters`: optional list of the input parameters in user-facing order, deepest
  *       stack item first. Each entry has a `name`, `type`, and optional `description`. The
  *       type must be the name of a known data type, e.g. `Query` or `TimeSeriesExpr`. If
  *       present, then `outputs` must also be specified.
  *     - `outputs`: list of output types pushed on to the stack by the operator. If
  *       present, then `parameters` must also be specified.
  *
  *     If the types are declared, then the word will be typed so that the stack matching
  *     and signature are based on the declared types. The examples will be checked when the
  *     vocabulary is loaded to verify that the body is consistent with the declared types,
  *     both the number of items consumed and pushed and the output types. Examples that are
  *     expected to fail, prefixed with `ERROR:`, are not checked.
  *
  *     ```
  *     words = [
  *       {
  *         name = "square"
  *         body = ":dup,:mul"
  *         summary = "Compute the square of the input."
  *         parameters = [
  *           {
  *             name = ""
  *             type = "TimeSeriesExpr"
  *             description = "input expression"
  *           }
  *         ]
  *         outputs = ["TimeSeriesExpr"]
  *         examples = ["2"]
  *       }
  *     ]
  *     ```
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
    val ws = macros ::: averages
    validateTypedMacros(ws, dependsOn.flatMap(_.allWords) ::: ws)
    ws
  }

  private def loadCustomWords(configs: List[Config]): List[Word] = {
    configs.map { cfg =>
      val name = cfg.getString("name")
      val body = Interpreter.splitAndTrim(cfg.getString("body"))
      val examples = cfg.getStringList("examples").asScala.toList
      val untyped = StandardVocabulary.Macro(name, body, examples)
      if (cfg.hasPath("parameters") || cfg.hasPath("outputs")) {
        val parameters = cfg.getConfigList("parameters").asScala.toIndexedSeq.map { p =>
          val desc = if (p.hasPath("description")) p.getString("description") else ""
          Parameter(p.getString("name"), desc, dataType(name, p.getString("type")))
        }
        val outputs = cfg.getStringList("outputs").asScala.toIndexedSeq.map(dataType(name, _))
        val summary = if (cfg.hasPath("summary")) cfg.getString("summary") else untyped.summary
        TypedMacro(name, body, parameters, outputs, summary, examples)
      } else {
        untyped
      }
    }
  }

  /**
    * Verify the examples for typed macros are consistent with the declared types. Otherwise
    * the signatures shown to users could be misleading.
    */
  private def validateTypedMacros(ws: List[Word], allWords: List[Word]): Unit = {
    lazy val interpreter = Interpreter(allWords)
    ws.foreach {
      case m: TypedMacro => m.examples.foreach(example => validateExample(interpreter, m, example))
      case _             =>
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

  /** Data types that can be referenced by name when declaring the types for custom words. */
  private val dataTypes: Map[String, DataType] = List(
    DataType.AnyType,
    DataType.IntType,
    DataType.DoubleType,
    DataType.StringType,
    DataType.ColorStringType,
    DataType.ListType,
    DataType.DurationType,
    DataType.StringListType,
    DataType.DoubleListType,
    ModelDataTypes.QueryType,
    ModelDataTypes.RollingWindowType,
    ModelDataTypes.ConsolidationFunctionType,
    ModelDataTypes.AggrType,
    ModelDataTypes.DataExprType,
    ModelDataTypes.TimeSeriesExprType,
    ModelDataTypes.PresentationType,
    ModelDataTypes.EventExprType,
    ModelDataTypes.PaletteType
  ).map(t => t.name -> t).toMap

  private def dataType(word: String, name: String): DataType = {
    dataTypes.getOrElse(
      name, {
        val known = dataTypes.keys.toList.sorted.mkString(", ")
        throw new IllegalArgumentException(
          s"unknown type '$name' for :$word, should be one of: $known"
        )
      }
    )
  }

  /**
    * Check that applying a typed word to the stack for an example is consistent with the
    * declared parameters and outputs. The word is applied directly rather than by name via
    * the interpreter, so that a word with the same name from a dependency cannot match
    * instead. Examples that are expected to fail, i.e. prefixed with `ERROR:`, are ignored.
    * Examples that use words that are not available are also ignored. The same custom words
    * can be loaded with different dependencies, e.g. the event vocabulary, where some of the
    * operators used in the body will not be present.
    */
  private[model] def validateExample(
    interpreter: Interpreter,
    w: TypedWord,
    example: String
  ): Unit = {
    if (!example.startsWith("ERROR:")) {
      try {
        val context = interpreter.execute(example, features = Features.UNSTABLE)
        if (!w.matches(context.stack)) {
          throw new IllegalArgumentException(
            s"example '$example' for :${w.name} does not match declared signature " +
              s"[${w.signature}]: ${context.stack.reverse.mkString(" ")}"
          )
        }
        val stack = w.execute(context).stack
        val expectedDepth = context.stack.length - w.parameters.length + w.outputs.length
        if (stack.length != expectedDepth) {
          throw new IllegalArgumentException(
            s"stack effect of :${w.name} for example '$example' does not match declared " +
              s"signature [${w.signature}]: expected $expectedDepth items, got ${stack.length}"
          )
        }
        val outputs = stack.take(w.outputs.length).reverse
        if (!outputs.zip(w.outputs).forall(t => t._2.extract(t._1).isDefined)) {
          val expected = w.outputs.map(_.name).mkString(" ")
          throw new IllegalArgumentException(
            s"output of :${w.name} for example '$example' does not match declared " +
              s"outputs [$expected]: ${outputs.mkString(" ")}"
          )
        }
      } catch {
        case _: UnknownWordException =>
      }
    }
  }

  private def eval(s: String): Query = {
    queryInterpreter.execute(s).stack match {
      case (q: Query) :: Nil => q
      case _                 => throw new IllegalArgumentException(s)
    }
  }

  case class CustomAvg(name: String, baseQuery: Query, keys: Set[String])
      extends TypedWord
      with Function2[Expr, List[String], Expr] {

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("q", "query for the numerator", ModelDataTypes.QueryType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(ModelDataTypes.TimeSeriesExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val q = params(0).asInstanceOf[Query]
      val nq = extractCommonQuery(q)
      val numerator = DataExpr.Sum(q)
      val denominator = DataExpr.Sum(baseQuery.and(nq))
      val avg = MathExpr.Divide(numerator, denominator)
      val ctxt = Context(context.interpreter, Nil, Map.empty)
      val rewrite = Some(this)
      val expr = MathExpr.NamedRewrite(name, q, Nil, avg, ctxt, rewrite)
      context.copy(stack = expr :: context.stack)
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

    override def examples: List[String] = Nil
  }
}
