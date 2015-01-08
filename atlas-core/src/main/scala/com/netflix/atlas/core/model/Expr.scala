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
package com.netflix.atlas.core.model

trait Expr extends Product {

  /**
   * Returns a string that can be executed with the stack interpreter to create this expression.
   */
  def exprString: String = toString

  /**
   * Rewrite the expression using the specified function. The default implementation will try to
   * recursively apply the rewrite to case classes.
   */
  def rewrite(f: PartialFunction[Expr, Expr]): Expr = {
    if (f.isDefinedAt(this)) f(this) else {
      this match {
        case p: Product =>
          val params = p.productIterator.map {
            case e: Expr => e.rewrite(f)
            case v       => v.asInstanceOf[AnyRef]
          }
          val ctors = getClass.getConstructors
          ctors(0).newInstance(params.toArray: _*).asInstanceOf[Expr]
        case v => v
      }
    }
  }
}
