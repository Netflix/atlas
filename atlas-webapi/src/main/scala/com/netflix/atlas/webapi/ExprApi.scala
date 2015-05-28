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
package com.netflix.atlas.webapi

import akka.actor.ActorRefFactory
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.json.Json
import spray.http.HttpEntity
import spray.http.HttpResponse
import spray.http.MediaTypes
import spray.http.StatusCodes
import spray.routing.RequestContext

/**
 * Generates a list of steps for executing an expression. This endpoint is typically used for
 * validating or debugging an expression.
 */
class ExprApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val vocabulary = ApiSettings.graphVocabulary

  private val vocabularies = {
    vocabulary.dependencies.map(v => v.name -> v).toMap + (vocabulary.name -> vocabulary)
  }

  def routes: RequestContext => Unit = {
    path("api" / "v1" / "expr") {
      get { ctx =>
        try processRequest(ctx) catch handleException(ctx)
      }
    }
  }

  private def newInterpreter(name: String): Interpreter = {
    val vocab = vocabularies(name)
    new Interpreter(vocab.allWords)
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

  private def processRequest(ctx: RequestContext): Unit = {
    val query = ctx.request.uri.query.get("q").getOrElse {
      throw new IllegalArgumentException("missing required parameter 'q'")
    }

    val vocabName = ctx.request.uri.query.getOrElse("vocab", vocabulary.name)

    val interpreter = newInterpreter(vocabName)
    val steps = interpreter.debug(query).map { step =>
      val stack = step.context.stack.map(valueString)
      val vars = step.context.variables.map(t => t._1 -> valueString(t._2))
      val ctxt = Map("stack" -> stack, "variables" -> vars)
      Map("program" -> step.program, "context" -> ctxt)
    }

    val data = Json.encode(steps)
    val entity = HttpEntity(MediaTypes.`application/json`, data)
    ctx.responder ! HttpResponse(StatusCodes.OK, entity = entity)
  }
}
