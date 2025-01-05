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

/**
  * A small procedure that manipulates the stack.
  */
trait Word {

  /** Name used to refer to this command. */
  def name: String

  /** Signature of the method showing the before and after effect on the stack. */
  def signature: String

  /** Short description of the word to help the user understand what it does. */
  def summary: String

  /**
    * Set of examples showing the usage of the word.
    */
  def examples: List[String]

  /**
    * Returns true if this operation is considered stable. New operations should override
    * this method to return false until the API is finalized.
    */
  def isStable: Boolean = true

  /**
    * Check if the this word can be executed against the current stack. Can be used as a basis for
    * finding auto-completion candidates.
    */
  def matches(stack: List[Any]): Boolean

  /** Execute this command against the provided context. */
  def execute(context: Context): Context

  /** Throw an exception to indicate an invalid stack was passed into the execution. */
  protected def invalidStack: Nothing = {
    throw new IllegalStateException(s"invalid stack for :$name")
  }
}

trait SimpleWord extends Word {

  def matches(stack: List[Any]): Boolean = {
    if (matcher.isDefinedAt(stack)) matcher(stack) else false
  }

  def execute(context: Context): Context = {
    if (executor.isDefinedAt(context.stack))
      context.copy(stack = executor(context.stack))
    else
      invalidStack
  }

  protected def matcher: PartialFunction[List[Any], Boolean]

  protected def executor: PartialFunction[List[Any], List[Any]]

}
