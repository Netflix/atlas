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
package com.netflix.atlas.core.stacklang

/**
  * State related to the execution of a stack language expression.
  *
  * @param interpreter
  *     Interpreter that is performing the execution.
  * @param stack
  *     Stack that maintains the state for the program.
  * @param variables
  *     Variables that can be set to keep state outside of the main stack. See the
  *     `:get` and `:set` operators for more information.
  * @param frozenStack
  *     Separate stack that has been frozen to prevent further modification. See the
  *     `:freeze` operator for more information.
  */
case class Context(
  interpreter: Interpreter,
  stack: List[Any],
  variables: Map[String, Any],
  frozenStack: List[Any] = Nil
) {

  /**
    * Remove the contents of the stack and push them onto the frozen stack. The variable
    * state will also be cleared.
    */
  def freeze: Context = {
    copy(stack = Nil, variables = Map.empty[String, Any], frozenStack = stack ::: frozenStack)
  }

  /**
    * Combine the stack and frozen stack to a final result stack. The frozen contents will
    * be older entries on the final result stack.
    */
  def unfreeze: Context = {
    copy(stack = stack ::: frozenStack, frozenStack = Nil)
  }
}
