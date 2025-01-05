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

import com.netflix.atlas.core.stacklang.StackItem

trait Expr extends Product with StackItem {

  /** Use builder for encoding as a string. Sub-classes should override the append method. */
  override def toString: String = {
    // 256 is a rough size that isn't too large to waste a lot, but also limit the
    // need for growing the buffer with typical expressions
    val builder = new java.lang.StringBuilder(256)
    append(builder)
    builder.toString
  }

  /**
    * Returns a string that can be executed with the stack interpreter to create this expression.
    */
  def exprString: String = toString

  /**
    * Rewrite the expression using the specified function. The default implementation will try to
    * recursively apply the rewrite to case classes.
    */
  def rewrite(f: PartialFunction[Expr, Expr]): Expr = {
    if (f.isDefinedAt(this)) f(this)
    else {
      this match {
        // If the productArity is 0 we cannot change instance so return the existing class. A
        // common case where the arity is 0 are case objects. If they go through the product
        // case it causes duplicate instances of the objects to get created leading to strange
        // failures in other places.
        case p: Product if p.productArity > 0 =>
          val params = p.productIterator.map {
            case e: Expr => e.rewrite(f)
            case v       => v.asInstanceOf[AnyRef]
          }
          val ctors = getClass.getConstructors
          ctors(0).newInstance(params.toArray*).asInstanceOf[Expr]
        case v => v
      }
    }
  }
}
