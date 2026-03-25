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
package com.netflix.atlas.webapi

import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.model.ExprNormalizer
import com.netflix.atlas.core.model.FilterExpr
import com.netflix.atlas.core.model.ModelDataTypes
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.util.Features
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json3.Json
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi

import scala.util.Try

/**
  * Generates a list of steps for executing an expression. This endpoint is typically used for
  * validating or debugging an expression.
  */
class ExprApi extends WebApi {

  private val vocabulary = ApiSettings.graphVocabulary

  private val vocabularies = {
    vocabulary.dependencies.map(v => v.name -> v).toMap + (vocabulary.name -> vocabulary)
  }

  private val excludedWords = ApiSettings.excludedWords

  def routes: Route = get {
    endpointPath("api" / "v1" / "expr") {
      parameters("q", "vocab" ? vocabulary.name) { (q, vocab) =>
        complete(processDebugRequest(q, vocab))
      }
    } ~
    pathPrefix("api" / "v1" / "expr") {
      parameters("q", "vocab" ? vocabulary.name) { (q, vocab) =>
        endpointPath("debug") {
          complete(processDebugRequest(q, vocab))
        } ~
        endpointPath("normalize") {
          complete(processNormalizeRequest(q, vocab))
        } ~
        endpointPath("complete") {
          complete(processCompleteRequest(q, vocab))
        } ~
        endpointPath("queries") {
          complete(processQueriesRequest(q, vocab))
        }
      } ~
      endpointPath("rewrite") {
        parameters("q") { q =>
          complete(processRewriteRequest(q))
        }
      } ~
      endpointPath("strip") {
        parameters("q", "k".repeated, "r".repeated) { (q, keys, vocabsToRemove) =>
          complete(processStripRequest(q, keys.toSet, vocabsToRemove.toSet))
        }
      }
    }
  }

  private def newInterpreter(name: String): Interpreter = {
    val vocab = vocabularies(name)
    new Interpreter(vocab.allWords)
  }

  private def verifyStackContents(vocab: String, stack: List[Any]): Unit = {
    vocab match {
      case "std" =>
      // Don't need to do anything, any stack should be considered valid
      case "query" =>
        // Expectation is that there would be a single query on the stack
        stack match {
          case _ :: Nil =>
          case _ :: _ =>
            val summary = Interpreter.typeSummary(stack)
            throw new IllegalArgumentException(s"expected a single query, found $summary")
          case Nil =>
            throw new IllegalArgumentException(s"expected a single query, stack is empty")
        }
      case _ =>
        // Expecting a style expression that can be used in a graph
        val invalidItem = stack.find {
          case ModelDataTypes.PresentationType(_) => false
          case _                                  => true
        }

        invalidItem.foreach { item =>
          val summary = Interpreter.typeSummary(List(item))
          throw new IllegalArgumentException(s"expected an expression, found $summary")
        }

        if (stack.isEmpty) {
          throw new IllegalArgumentException(s"expected an expression, stack is empty")
        }
    }
  }

  /**
    * Currently the values just get converted to a string as the automatic json mapping doesn't
    * provide enough context. Also, the formatter when laying out the expressions for the debug
    * view works well enough for displaying the expr strings to the user. The output can be
    * enhanced in a later version.
    */
  private def valueString(value: Any): String = value match {
    case v: Expr => v.exprString
    case v       => v.toString
  }

  private def processDebugRequest(query: String, vocabName: String): HttpResponse = {
    val interpreter = newInterpreter(vocabName)
    val execSteps = interpreter.debug(query)
    if (execSteps.nonEmpty) {
      verifyStackContents(vocabName, execSteps.last.context.stack)
    }

    val steps = execSteps.map { step =>
      val stack = step.context.stack.map(valueString)
      val vars = step.context.variables.map(t => t._1 -> valueString(t._2))
      val ctxt = Map("stack" -> stack, "variables" -> vars)
      Map("program" -> step.program, "context" -> ctxt)
    }
    jsonResponse(steps)
  }

  private def processNormalizeRequest(query: String, vocabName: String): HttpResponse = {
    val interpreter = newInterpreter(vocabName)
    jsonResponse(ExprApi.normalize(query, interpreter))
  }

  // This check is needed to be sure an operation will work if matches is not exhaustive. In
  // some cases it only validates types, but not acceptable values such as :time. For others like
  // macros it always returns true. This ensures the operation will actually be successful before
  // returning to a user.
  private def execWorks(interpreter: Interpreter, w: Word, ctxt: Context): Boolean = {
    Try(interpreter.executeProgram(List(s":${w.name}"), ctxt)).isSuccess
  }

  private def matches(interpreter: Interpreter, w: Word, ctxt: Context): Boolean = {
    !excludedWords.contains(w.name) && w.matches(ctxt.stack) && execWorks(interpreter, w, ctxt)
  }

  private def processCompleteRequest(query: String, vocabName: String): HttpResponse = {
    val interpreter = newInterpreter(vocabName)
    val result = interpreter.execute(query, features = Features.UNSTABLE)

    val candidates = interpreter.vocabulary.filter { w =>
      matches(interpreter, w, result)
    }
    val descriptions = candidates.map { w =>
      Map("name" -> w.name, "signature" -> w.signature, "description" -> w.summary)
    }
    jsonResponse(descriptions)
  }

  /**
    * Extract the queries[1] from a stack expression.
    *
    * This can be useful for UIs that need to further explore the tag space
    * associated with a graph expression. Output is a list of all distinct
    * queries used.
    *
    * [1] https://github.com/Netflix/atlas/wiki/Reference-query
    */
  private def processQueriesRequest(expr: String, vocabName: String): HttpResponse = {
    val interpreter = newInterpreter(vocabName)
    val result = interpreter.execute(expr, features = Features.UNSTABLE)

    val exprs = result.stack.collect {
      case ModelDataTypes.PresentationType(t) => t
    }
    val queries = exprs
      .flatMap(_.expr.dataExprs.map(_.query))
      .map(_.toString)
      .sortWith(_ < _)
      .distinct
    jsonResponse(queries)
  }

  /**
    * Strip query clauses that contain a key in the set.
    */
  private def processStripRequest(
    expr: String,
    keys: Set[String],
    vocabsToRemove: Set[String]
  ): HttpResponse = {
    val interpreter = newInterpreter(vocabulary.name)
    val result = interpreter.execute(expr)

    val exprs = result.stack.collect {
      case ModelDataTypes.PresentationType(t) =>
        stripVocabulary(t.rewrite(stripKeys(keys)), vocabsToRemove.toList).toString
    }

    jsonResponse(exprs)
  }

  private def stripKeys(keys: Set[String]): PartialFunction[Expr, Expr] = {
    case q: Query => Query.simplify(stripKeys(q, keys), ignore = true)
  }

  private def stripKeys(query: Query, ks: Set[String]): Query = query match {
    case Query.And(q1, q2)                     => Query.And(stripKeys(q1, ks), stripKeys(q2, ks))
    case Query.Or(q1, q2)                      => Query.Or(stripKeys(q1, ks), stripKeys(q2, ks))
    case Query.Not(q)                          => Query.Not(stripKeys(q, ks))
    case q: Query.KeyQuery if ks.contains(q.k) => Query.True
    case q                                     => q
  }

  @scala.annotation.tailrec
  private def stripVocabulary(expr: Expr, vocabsToRemove: List[String]): Expr = {
    vocabsToRemove match {
      case "filter" :: vs => stripVocabulary(stripFilter(expr), vs)
      case "style" :: vs  => stripVocabulary(stripStyle(expr), vs)
      case v :: _         => throw new IllegalArgumentException(s"vocabulary '$v' not supported")
      case Nil            => expr
    }
  }

  private def stripFilter(expr: Expr): Expr = {
    expr.rewrite {
      case FilterExpr.Stat(e, _, _)         => e
      case FilterExpr.Filter(e, _)          => e
      case e: FilterExpr.PriorityFilterExpr => e.expr
    }
  }

  private def stripStyle(expr: Expr): Expr = {
    expr match {
      case e: StyleExpr => e.expr
      case e            => e
    }
  }

  /**
    * Rewrite a graph expression to phase out deprecated usage. Currently, it will
    * only rewrite the legacy offset usage.
    */
  private def processRewriteRequest(expr: String): HttpResponse = {
    val interpreter = Interpreter(vocabulary.allWords)
    val rewrittenExprs = ExprApi.rewrite(expr, interpreter)
    jsonResponse(rewrittenExprs)
  }

  /** Encode `obj` as json and create the HttpResponse. */
  private def jsonResponse(obj: AnyRef): HttpResponse = {
    val data = Json.encode(obj)
    val entity = HttpEntity(MediaTypes.`application/json`, data)
    HttpResponse(StatusCodes.OK, entity = entity)
  }
}

object ExprApi {

  private val normalizer = new ExprNormalizer(ApiSettings.normalizeConfig)

  /**
    * Normalizes an Atlas expression program into a canonical string representation.
    *
    * This method parses the program string using the provided interpreter, extracts
    * style expressions from the stack, and normalizes them into consistent, comparable
    * string forms. Normalization includes:
    *
    *  - Standardizing legend variable syntax to always use parentheses: `$(var)`
    *  - Sorting query clauses by importance (prefix keys, regular keys, suffix keys)
    *  - Removing redundant clauses and simplifying queries
    *  - Converting stat filters to their canonical forms
    *  - Removing explicit `:const` and `:line` suffixes
    *
    * @param program
    *     The Atlas expression program string to normalize (e.g., "name,cpu,:eq,:avg")
    * @param interpreter
    *     The interpreter configured with the appropriate vocabulary for parsing
    * @return
    *     A list of normalized expression strings in user-expected order (reversed from stack order)
    */
  def normalize(program: String, interpreter: Interpreter): List[String] = {
    normalize(eval(interpreter, program))
  }

  /**
    * Normalizes a list of style expressions into canonical string representations.
    *
    * This overload operates directly on parsed style expressions rather than a program string.
    * It applies the same normalization transformations as the string-based version.
    *
    * @param exprs
    *     The list of style expressions to normalize
    * @return
    *     A list of normalized expression strings in user-expected order (reversed from stack order)
    */
  def normalize(exprs: List[StyleExpr]): List[String] = {
    exprs.map(normalizer.normalizeToString).reverse
  }

  private def eval(interpreter: Interpreter, expr: String): List[StyleExpr] = {
    interpreter.execute(expr, features = Features.UNSTABLE).stack.collect {
      case ModelDataTypes.PresentationType(t) => t
    }
  }

  /**
    * Rewrites an Atlas expression to phase out deprecated usage patterns.
    *
    * Currently focuses on rewriting legacy offset usage to the canonical form:
    *
    *  - Single zero offset: Removes the offset setting entirely
    *  - Single non-zero offset: Converts to `:offset` operation (e.g., `expr,1h,:offset`)
    *  - Multiple offsets: Extracts the base expression into a variable and applies each offset
    *    separately (e.g., `Query0,expr,:set,Query0,:get,Query0,:get,1h,:offset`)
    *
    * This is useful for migrating expressions that use the deprecated style-based offset
    * settings to the newer operator-based approach.
    *
    * @param expr
    *     The Atlas expression string to rewrite
    * @param interpreter
    *     The interpreter configured with the graph vocabulary for parsing
    * @return
    *     A list of rewritten expression strings, one per style expression in the input
    */
  def rewrite(expr: String, interpreter: Interpreter): List[String] = {
    val result = interpreter.execute(expr, features = Features.UNSTABLE)

    val exprs = result.stack.collect {
      case ModelDataTypes.PresentationType(t) => t
    }

    exprs.zipWithIndex.map(t => rewriteOffset(t._1, t._2))
  }

  private def rewriteOffset(expr: StyleExpr, i: Int): String = {
    expr.styleOffsets match {
      case Nil =>
        expr.toString
      case d :: Nil if d.isZero =>
        removeOffsets(expr).toString
      case d :: Nil =>
        s"${removeOffsets(expr)},${Strings.toString(d)},:offset"
      case ds =>
        val varName = s"Query$i"
        val baseExpr = s"$varName,${removeOffsets(expr)},:set"
        val offsets = ds
          .map { d =>
            if (d.isZero)
              s"$varName,:get"
            else
              s"$varName,:get,${Strings.toString(d)},:offset"
          }
          .mkString(",")
        s"$baseExpr,$offsets"
    }
  }

  private def removeOffsets(expr: StyleExpr): StyleExpr = {
    expr.copy(settings = expr.settings - "offset")
  }
}
