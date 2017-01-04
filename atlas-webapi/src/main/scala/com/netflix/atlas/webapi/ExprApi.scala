/*
 * Copyright 2014-2017 Netflix, Inc.
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

import akka.actor.ActorRefFactory
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.model.MathExpr
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TimeSeriesExpr
import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.json.Json
import spray.http.HttpEntity
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCodes
import spray.routing.RequestContext

import scala.util.Try

/**
 * Generates a list of steps for executing an expression. This endpoint is typically used for
 * validating or debugging an expression.
 */
class ExprApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val vocabulary = ApiSettings.graphVocabulary

  private val vocabularies = {
    vocabulary.dependencies.map(v => v.name -> v).toMap + (vocabulary.name -> vocabulary)
  }

  private val excludedWords = ApiSettings.excludedWords

  def routes: RequestContext => Unit = {
    path("api" / "v1" / "expr") {
      get { ctx => processDebugRequest(ctx) }
    } ~
    pathPrefix("api" / "v1" / "expr") {
      path("debug") {
        get { ctx => processDebugRequest(ctx) }
      } ~
      path("normalize") {
        get { ctx => processNormalizeRequest(ctx) }
      } ~
      path("complete") {
        get { ctx => processCompleteRequest(ctx) }
      } ~
      path("queries") {
        get { ctx => processQueriesRequest(ctx) }
      }
    }
  }

  private def newInterpreter(name: String): Interpreter = {
    val vocab = vocabularies(name)
    new Interpreter(vocab.allWords)
  }

  private def verifyStackContents(vocab: String, stack: List[Any]): Unit = {
    vocab match {
      case "std"   =>
        // Don't need to do anything, any stack should be considered valid
      case "query" =>
        // Expectation is that there would be a single query on the stack
        stack match {
          case v :: Nil =>
          case v :: vs =>
            val summary = Interpreter.typeSummary(stack)
            throw new IllegalArgumentException(s"expected a single query, found $summary")
          case Nil =>
            throw new IllegalArgumentException(s"expected a single query, stack is empty")
        }
      case _ =>
        // Expecting a style expression that can be used in a graph
        val invalidItem = stack.find {
          case ModelExtractors.PresentationType(_) => false
          case _ => true
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

  private def getInterpreter(ctx: RequestContext): (String, Interpreter) = {
    val query = ctx.request.uri.query.get("q").getOrElse {
      throw new IllegalArgumentException("missing required parameter 'q'")
    }
    val vocabName = ctx.request.uri.query.getOrElse("vocab", vocabulary.name)
    val interpreter = newInterpreter(vocabName)
    query -> interpreter
  }

  private def processDebugRequest(ctx: RequestContext): Unit = {
    val (query, interpreter) = getInterpreter(ctx)
    val vocabName = ctx.request.uri.query.getOrElse("vocab", vocabulary.name)
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
    sendJson(ctx, steps)
  }

  private def processNormalizeRequest(ctx: RequestContext): Unit = {
    val (query, interpreter) = getInterpreter(ctx)
    sendJson(ctx, ExprApi.normalize(query, interpreter))
  }

  // This check is needed to be sure an operation will work if matches is not exhaustive. In
  // some cases it only validates types, but not acceptable values such as :time. For others like
  // macros it alwasy returns true. This ensures the operation will actually be successful before
  // returning to a user.
  private def execWorks(interpreter: Interpreter, w: Word, ctxt: Context): Boolean = {
    Try(interpreter.execute(List(s":${w.name}"), ctxt)).isSuccess
  }

  private def matches(interpreter: Interpreter, w: Word, ctxt: Context): Boolean = {
    !excludedWords.contains(w.name) && w.matches(ctxt.stack) && execWorks(interpreter, w, ctxt)
  }

  private def processCompleteRequest(ctx: RequestContext): Unit = {
    val (query, interpreter) = getInterpreter(ctx)
    val result = interpreter.execute(query)

    val candidates = interpreter.vocabulary.filter { w => matches(interpreter, w, result) }
    val descriptions = candidates.map { w =>
      Map("name" -> w.name, "signature" -> w.signature, "description" -> w.summary)
    }
    sendJson(ctx, descriptions)
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
  private def processQueriesRequest(ctx: RequestContext): Unit = {
    val (expr, interpreter) = getInterpreter(ctx)
    val result = interpreter.execute(expr)

    val exprs = result.stack.collect {
      case ModelExtractors.PresentationType(t) => t
    }
    val queries = exprs
      .flatMap(_.expr.dataExprs.map(_.query))
      .map(_.toString)
      .sortWith(_ < _)
      .distinct
    sendJson(ctx, queries)
  }

  /** Encode `obj` as json and send to `ctx.responder`. */
  private def sendJson(ctx: RequestContext, obj: AnyRef): Unit = {
    val data = Json.encode(obj)
    val entity = HttpEntity(MediaTypes.`application/json`, data)
    ctx.responder ! HttpResponse(StatusCodes.OK, entity = entity)
  }
}

object ExprApi {

  def normalize(program: String, interpreter: Interpreter): List[String] = {
    normalize(eval(interpreter, program), interpreter)
  }

  def normalize(exprs: List[StyleExpr], interpreter: Interpreter): List[String] = {
    // If named rewrites are used, then map the eval expression to match the display
    // expression. This avoids the complexity of the eval expression showing up in the
    // extracted data expressions.
    import MathExpr.NamedRewrite
    val cleaned = exprs.map { e =>
      val clean = e.rewrite {
        case NamedRewrite(n, orig: Query,          _) => NamedRewrite(n, orig, DataExpr.Sum(orig))
        case NamedRewrite(n, orig: TimeSeriesExpr, _) => NamedRewrite(n, orig, orig)
      }
      clean.asInstanceOf[StyleExpr]
    }

    // Extract the distinct queries from all data expressions
    val queries = cleaned.flatMap(_.expr.dataExprs.map(_.query)).distinct

    // Map from original query to CNF form. This is used to find the common clauses that
    // can be extracted and applied to the overall query using :cq.
    val cnfQueries = queries.map(q => q -> Query.cnfList(q).toSet).toMap

    // Find the set of common query clauses
    val commonQueries =
      if (cnfQueries.isEmpty) Set.empty[Query]
      else cnfQueries.values.reduceLeft { (s1, s2) => s1.intersect(s2) }

    // Normalize without extracting common queries
    val normalized = exprStrings(exprs, cnfQueries, Set.empty)
    val fullExpr = normalized.mkString(",")

    // Normalize with extracting common queries
    val cq = s":list,(,${sort(commonQueries, Set.empty)},:cq,),:each"
    val normalizedCQ = cq :: exprStrings(exprs, cnfQueries, commonQueries)
    val fullExprCQ = normalizedCQ.mkString(",")

    // If needed, then prepend a common query conversion to the list
    val finalList = if (fullExpr.length < fullExprCQ.length) normalized else normalizedCQ

    // Reverse the list to match the order the user would expect
    finalList.reverse
  }

  private def eval(interpreter: Interpreter, expr: String): List[StyleExpr] = {
    interpreter.execute(expr).stack.collect {
      case ModelExtractors.PresentationType(t) => t
    }
  }

  private def exprStrings(exprs: List[StyleExpr], cnf: Map[Query, Set[Query]], cq: Set[Query]): List[String] = {
    // Map from original query to sorted query without the common clauses excluded
    val sortedQueries = cnf.map { case (q, qs) => q -> sort(qs, cq) }

    // Rewrite the expressions and convert to a normalized strings
    exprs.map { expr =>
      val rewritten = expr.rewrite {
        case q: Query => sortedQueries.getOrElse(q, q)
      }
      // Remove explicit :const, it can be determined from implicit conversion
      // and adds visual clutter
      rewritten.toString.replace(",:const", "")
    }
  }

  /**
    * Combines a set of query clauses together using AND. Common query clauses that can
    * be applied later using :cq can be added to the exclude set so they will get ignored
    * here. The clauses will be sorted so any queries with the exact same set of clauses
    * will have an equal result query even if they were in different orders in the input
    * expression string.
    */
  private def sort(qs: Set[Query], exclude: Set[Query]): Query = {
    val matches = qs.toList.filter(q => !exclude.contains(q))
    if (matches.isEmpty)
      Query.True
    else
      matches.sortWith(_.toString < _.toString).reduce { (q1, q2) => Query.And(q1, q2) }
  }
}
