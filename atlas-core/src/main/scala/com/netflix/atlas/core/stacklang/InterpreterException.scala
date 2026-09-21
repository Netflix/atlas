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
package com.netflix.atlas.core.stacklang

/**
  * Failure to resolve a word while executing a program, carrying the context the interpreter
  * had at the point it gave up. The failing word and the stack it was reached with are exposed
  * as fields so a caller can act on them, for example to report which words would have been
  * valid at that point rather than only that the expression was rejected.
  *
  * Extends `IllegalStateException`, which is what these throw sites have always thrown, so
  * `catch` blocks and the 4xx/5xx classification in `RequestHandler` and `GraphRequestActor`
  * are unaffected. The type name is not purely internal though: anything that renders
  * `getClass.getSimpleName`, such as `DiagnosticMessage` for the message on an error response
  * and the `error` tag on `atlas.graph.errorImages`, reports the subclass name instead of
  * `IllegalStateException`.
  *
  * Note that these do not carry the position of the failing token, so a caller that needs to
  * point at the offending text should use [[Interpreter.syntaxTree]], which recovers from the
  * failure and records a span for each diagnostic.
  */
sealed abstract class InterpreterException(message: String) extends IllegalStateException(message)

object UnknownWordException {

  /**
    * Message used for a word that is not in the vocabulary. Shared with the recovering
    * [[Interpreter.syntaxTree]] path so both report the failure identically and callers
    * matching on the text only have one form to handle.
    */
  def message(word: String): String = s"unknown word ':$word'"
}

/**
  * No word with this name exists in the vocabulary.
  *
  * @param word
  *     Name of the word, without the leading colon.
  * @param stack
  *     Stack as it stood when the word was reached. The words valid at that point are a
  *     function of this, so it is what a caller needs to suggest an alternative.
  */
final class UnknownWordException(val word: String, val stack: List[Any])
    extends InterpreterException(UnknownWordException.message(word))

/**
  * The word exists, but none of its overloads accept the stack it was applied to.
  *
  * @param word
  *     Name of the word, without the leading colon.
  * @param stack
  *     Stack the word was applied to.
  * @param signatures
  *     Signatures of the overloads that were tried, in the order they were considered.
  */
final class StackMismatchException(
  val word: String,
  val stack: List[Any],
  val signatures: List[String]
) extends InterpreterException(
      s"no matches for word ':$word' with stack ${Interpreter.typeSummary(stack)}, " +
        s"candidates: ${signatures.mkString("[", "], [", "]")}"
    )
