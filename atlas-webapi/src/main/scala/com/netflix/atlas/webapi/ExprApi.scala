/*
 * Copyright 2014-2016 Netflix, Inc.
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
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.model.ModelExtractors
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

    val data = Json.encode(steps)
    val entity = HttpEntity(MediaTypes.`application/json`, data)
    ctx.responder ! HttpResponse(StatusCodes.OK, entity = entity)
  }

  private def processNormalizeRequest(ctx: RequestContext): Unit = {
    val (query, interpreter) = getInterpreter(ctx)
    val result = interpreter.execute(query)
    val data = Json.encode(result.stack.reverse.map(_.toString))
    val entity = HttpEntity(MediaTypes.`application/json`, data)
    ctx.responder ! HttpResponse(StatusCodes.OK, entity = entity)
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

    val data = Json.encode(descriptions)
    val entity = HttpEntity(MediaTypes.`application/json`, data)
    ctx.responder ! HttpResponse(StatusCodes.OK, entity = entity)
  }
}
