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
package com.netflix.atlas.core.stacklang

import com.netflix.atlas.core.util.Features

/**
  * Interpreter for stack expressions.
  *
  * @param vocabulary
  *     Set of supported words. If multiple words have the same name, then the first one that matches
  *     with the current stack will get used.
  */
case class Interpreter(vocabulary: List[Word]) {

  import com.netflix.atlas.core.stacklang.Interpreter._

  // Map of name to word
  private val words = vocabulary.groupBy(_.name)

  @scala.annotation.tailrec
  private def executeFirstMatchingWordImpl(ws: List[Word], context: Context): Option[Context] = {
    ws match {
      case v :: vs =>
        if (v.matches(context.stack)) {
          if (!v.isStable && context.features != Features.UNSTABLE) {
            throw new IllegalStateException(s"to use :${v.name} enable unstable features")
          }
          Some(v.execute(context))
        } else {
          executeFirstMatchingWordImpl(vs, context)
        }
      case Nil =>
        None
    }
  }

  private def executeFirstMatchingWord(name: String, ws: List[Word], context: Context): Context = {
    executeFirstMatchingWordImpl(ws, context).getOrElse {
      val stackSummary = Interpreter.typeSummary(context.stack)
      val candidates = ws.map(_.signature).mkString("[", "], [", "]")
      throw new IllegalStateException(
        s"no matches for word ':$name' with stack $stackSummary, candidates: $candidates"
      )
    }
  }

  private def executeWord(name: String, context: Context): Context = {
    words.get(name) match {
      case Some(ws) => executeFirstMatchingWord(name, ws, context)
      case None     => throw new IllegalStateException(s"unknown word ':$name'")
    }
  }

  /**
    * Called with the remaining items from the program after an opening parenthesis is found. It
    * will push a list containing all items until the matching closing parenthesis. Commands inside
    * the list will not get executed.
    */
  @scala.annotation.tailrec
  private def popAndPushList(depth: Int, acc: List[Any], step: Step): Step = {
    step.program match {
      case "(" :: tokens =>
        popAndPushList(depth + 1, "(" :: acc, step.copy(program = tokens))
      case ")" :: tokens if depth == 0 =>
        val context = step.context
        Step(tokens, context.copy(stack = acc.reverse :: context.stack))
      case ")" :: tokens if depth > 0 =>
        popAndPushList(depth - 1, ")" :: acc, step.copy(program = tokens))
      case t :: tokens =>
        popAndPushList(depth, t :: acc, step.copy(program = tokens))
      case Nil =>
        throw new IllegalStateException("unmatched opening parenthesis")
    }
  }

  private def nextStep(s: Step): Step = {
    val context = s.context
    s.program match {
      case token :: tokens =>
        token match {
          case "("       => popAndPushList(0, Nil, Step(tokens, context))
          case ")"       => throw new IllegalStateException("unmatched closing parenthesis")
          case IsWord(t) => Step(tokens, executeWord(t, context))
          case t         => Step(tokens, context.copy(stack = t :: context.stack))
        }
      case Nil => s
    }
  }

  @scala.annotation.tailrec
  private def execute(s: Step): Context = {
    if (s.program.isEmpty) s.context else execute(nextStep(s))
  }

  final def executeProgram(
    program: List[Any],
    context: Context,
    unfreeze: Boolean = true
  ): Context = {
    val result = execute(Step(program, context))
    if (unfreeze) result.unfreeze else result
  }

  final def executeProgram(program: List[Any]): Context = {
    executeProgram(program, Context(this, Nil, Map.empty))
  }

  final def execute(
    program: String,
    vars: Map[String, Any] = Map.empty,
    features: Features = Features.STABLE
  ): Context = {
    executeProgram(splitAndTrim(program), Context(this, Nil, vars, vars, features = features))
  }

  @scala.annotation.tailrec
  private def debugImpl(steps: List[Step], s: Step): List[Step] = {
    val trace = s :: steps
    if (s.program.isEmpty) trace else debugImpl(trace, nextStep(s))
  }

  final def debug(program: List[Any], context: Context): List[Step] = {
    val result = debugImpl(Nil, Step(program, context)) match {
      case s :: steps => s.copy(context = s.context.unfreeze) :: steps
      case Nil        => Nil
    }
    result.reverse
  }

  final def debug(program: List[Any]): List[Step] = {
    debug(program, Context(this, Nil, Map.empty))
  }

  final def debug(program: String): List[Step] = {
    debug(splitAndTrim(program))
  }

  /**
    * Return a list of all words in the vocabulary for this interpreter that match the provided
    * stack.
    */
  final def candidates(stack: List[Any]): List[Word] = {
    vocabulary.filter(_.matches(stack))
  }

  /**
    * Return a list of overloaded variants that match. The first word in the list is what would
    * get used when executing.
    */
  final def candidates(name: String, stack: List[Any]): List[Word] = {
    words(name).filter(_.matches(stack))
  }

  override def toString: String = s"Interpreter(${vocabulary.size} words)"
}

object Interpreter {

  case class Step(program: List[Any], context: Context)

  case object IsWord {

    def unapply(v: String): Option[String] = if (v.startsWith(":")) Some(v.substring(1)) else None
  }

  /**
    * List classes show simple names like "Nil$" and "colon$colon$" that are confusing to most
    * looking at the output. This tries to detect the list and use the simple name "List".
    */
  private def getTypeName(v: Any): String = v match {
    case _: List[_] => "List"
    case _          => v.getClass.getSimpleName
  }

  def typeSummary(stack: List[Any]): String = {
    stack.reverse.map(getTypeName).mkString("[", ",", "]")
  }

  def toString(stack: List[Any]): String = toStringImpl(stack.reverse)

  private def toStringImpl(items: List[Any]): String = {
    val parts = items.map {
      case vs: List[_] => s"(,${toStringImpl(vs)},)"
      case v           => v.toString
    }
    parts.mkString(",")
  }

  /**
    * Helper for efficiently splitting the input string. See StringSplit benchmark for
    * comparison with more naive approach. The string split method optimizes simple cases
    * with a single character so that it does not require compiling the regex. This method
    * uses a single character and does a simple `trim` operation to cleanup the whitespace.
    */
  def splitAndTrim(str: String): List[String] = {
    val parts = str.split(",")
    val builder = List.newBuilder[String]
    var i = 0
    while (i < parts.length) {
      val tmp = parts(i).trim
      if (!tmp.isEmpty)
        builder += tmp
      i += 1
    }
    builder.result()
  }
}
