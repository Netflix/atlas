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

object StandardVocabulary extends Vocabulary {

  val name: String = "std"

  val dependsOn: List[Vocabulary] = Nil

  val words: List[Word] = List(
    Call,
    Clear,
    Depth,
    Drop,
    Dup,
    Each,
    Format,
    Freeze,
    Get,
    Pick,
    Map,
    NDrop,
    NList,
    Over,
    ReverseRot,
    Roll,
    Rot,
    Set,
    Swap,
    ToList,
    Macro("2over", List(":over", ":over"), List("a,b")),
    Macro("nip", List(":swap", ":drop"), List("a,b")),
    Macro("tuck", List(":swap", ":over"), List("a,b")),
    Macro("fcall", List(":get", ":call"), List("duplicate,(,:dup,),:set,a,duplicate")),
    Macro("sset", List(":swap", ":set"), List("a,b"))
  )

  import Extractors.*

  /** A word defined as a sequence of other commands. */
  case class Macro(name: String, body: List[Any], examples: List[String] = Nil) extends Word {

    override def matches(stack: List[Any]): Boolean = true

    override def execute(context: Context): Context = {
      context.interpreter.executeProgram(body, context, unfreeze = false)
    }

    override def summary: String =
      s"""
        |Shorthand equivalent to writing: `${Interpreter.toString(body.reverse)}`
      """.stripMargin.trim

    override def signature: String = "? -- ?"
  }

  /** Pop a list off the stack and execute it as a program. */
  case object Call extends Word {

    override def name: String = "call"

    override def matches(stack: List[Any]): Boolean = stack match {
      case (_: List[?]) :: _ => true
      case _                 => false
    }

    override def execute(context: Context): Context = {
      context.stack match {
        case (vs: List[?]) :: stack =>
          context.interpreter.executeProgram(vs, context.copy(stack = stack), unfreeze = false)
        case _ => invalidStack
      }
    }

    override def summary: String =
      """
        |Pop a list off the stack and execute it as a program.
      """.stripMargin.trim

    override def signature: String = "? List -- ?"

    override def examples: List[String] = List("(,a,)")
  }

  /** Remove all items from the stack. */
  case object Clear extends SimpleWord {

    override def name: String = "clear"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case _ => true }

    protected def executor: PartialFunction[List[Any], List[Any]] = { case _ => Nil }

    override def summary: String =
      """
        |Remove all items from the stack.
      """.stripMargin.trim

    override def signature: String = "* -- <empty>"

    override def examples: List[String] = List("a,b,c")
  }

  /** Compute the depth of the stack. */
  case object Depth extends SimpleWord {

    override def name: String = "depth"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case _ => true }

    protected def executor: PartialFunction[List[Any], List[Any]] = {

      // The depth is pushed as a string because we don't currently have a way to indicate the
      // type. The IntType extractor will match the string for operations that need to extract
      // and int.
      case vs => vs.size.toString :: vs
    }

    override def summary: String =
      """
        |Push the depth of the stack.
        |
        |Since: 1.5.0
      """.stripMargin.trim

    override def signature: String = " -- N"

    override def examples: List[String] = List("", "a", "a,b")
  }

  /** Remove the item on the top of the stack. */
  case object Drop extends SimpleWord {

    override def name: String = "drop"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case vs => vs.nonEmpty }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case _ :: s => s
    }

    override def summary: String =
      """
        |Remove the item on the top of the stack.
      """.stripMargin.trim

    override def signature: String = "a -- "

    override def examples: List[String] = List("a,b,c", "ERROR:")
  }

  /** Duplicate the item on the top of the stack. */
  case object Dup extends SimpleWord {

    override def name: String = "dup"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case vs => vs.nonEmpty }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case s @ v :: _ => v :: s
    }

    override def summary: String =
      """
        |Duplicate the item on the top of the stack.
      """.stripMargin.trim

    override def signature: String = "a -- a a"

    override def examples: List[String] = List("a", "a,b", "ERROR:")
  }

  /** For each item in a list push it on the stack and apply a function. */
  case object Each extends Word {

    override def name: String = "each"

    override def matches(stack: List[Any]): Boolean = stack match {
      case (_: List[?]) :: (_: List[?]) :: _ => true
      case _                                 => false
    }

    override def execute(context: Context): Context = {
      context.stack match {
        case (f: List[?]) :: (vs: List[?]) :: stack =>
          vs.reverse.foldLeft(context.copy(stack = stack)) { (c, v) =>
            c.interpreter.executeProgram(f, c.copy(stack = v :: c.stack), unfreeze = false)
          }
        case _ => invalidStack
      }
    }

    override def summary: String =
      """
        |For each item in a list push it on the stack and apply a function.
      """.stripMargin.trim

    override def signature: String = "items:List f:List -- f(items[0]) ... f(items[N])"

    override def examples: List[String] = List("(,a,b,),(,:dup,)")
  }

  /** Format a string. */
  case object Format extends SimpleWord {

    override def name: String = "format"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: List[?]) :: (_: String) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (vs: List[?]) :: (s: String) :: stack => s.format(vs*) :: stack
    }

    override def summary: String =
      """
        |Format a string using a printf [style pattern][formatter].
        |
        |[formatter]: https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html
      """.stripMargin.trim

    override def signature: String = "pattern:String args:List -- str:String"

    override def examples: List[String] = List("foo%s,(,bar,)")
  }

  /** Freeze the current contents of the stack so they cannot be modified. */
  case object Freeze extends Word {

    override def name: String = "freeze"

    override def matches(stack: List[Any]): Boolean = true

    override def execute(context: Context): Context = context.freeze

    override def summary: String =
      """
        |Freeze removes all data from the stack and pushes it to a separate frozen stack
        |that cannot be modified other than to push additional items using the freeze operation.
        |The final stack at the end of the execution will include the frozen contents along with
        |any thing that is on the normal stack.
        |
        |This operation is useful for isolating common parts of the stack while still allowing
        |tooling to manipulate the main stack using concatenative rewrite operations. The most
        |common example of this is the [:cq](math-cq) operation used to apply a common query
        |to graph expressions. For a concrete example, suppose you want to have an overlay
        |expression showing network errors on a switch that you want to add in to graphs on
        |a dashboard. The dashboard allows drilling into the graphs by selecting a particular
        |cluster. To make this work the dashboard appends a query rewrite to the expression
        |like:
        |
        |```
        |,:list,(,nf.cluster,{{selected_cluster}},:eq,:cq,),:each
        |```
        |
        |This [:list](std-list) operator will apply to everything on the stack. However, this
        |is problematic because the cluster restriction will break the overlay query. Using
        |the freeze operator the overlay expression can be isolated from the main stack. So
        |the final expression would look something like:
        |
        |```
        |# Query that should be used as is and not modified further
        |name,networkErrors,:eq,:sum,50,:gt,:vspan,40,:alpha,
        |:freeze,
        |
        |# Normal contents of the stack
        |name,ssCpuUser,:eq,:avg,1,:axis,
        |name,loadavg1,:eq,:avg,2,:axis,
        |
        |# Rewrite appended by tooling, only applies to main stack
        |:list,(,nf.cluster,{{selected_cluster}},:eq,:cq,),:each
        |```
        |
        |Since: 1.6
      """.stripMargin.trim

    override def signature: String = "* -- <empty>"

    override def examples: List[String] = List("a,b,c")
  }

  /** Get the value of a variable and push it on the stack. */
  case object Get extends Word {

    override def name: String = "get"

    override def matches(stack: List[Any]): Boolean = stack match {
      case (_: String) :: _ => true
      case _                => false
    }

    override def execute(context: Context): Context = {
      context.stack match {
        case (k: String) :: _ => context.copy(stack = context.variables(k) :: context.stack.tail)
        case _                => invalidStack
      }
    }

    override def summary: String =
      """
        |Get the value of a variable and push it on the stack.
      """.stripMargin.trim

    override def signature: String = "k -- vars[k]"

    override def examples: List[String] = List("k,v,:set,k")
  }

  /** Pick an item in the stack and put a copy on the top. */
  case object Pick extends SimpleWord {

    override def name: String = "pick"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case IntType(_) :: vs => vs.nonEmpty
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case IntType(n) :: vs => vs(n) :: vs
    }

    override def summary: String =
      """
        |Pick an item in the stack and put a copy on the top.
        |
        |Since: 1.5.0
      """.stripMargin.trim

    override def signature: String = "aN ... a0 N -- aN ... a0 aN"

    override def examples: List[String] = List("a,0", "a,b,0", "a,b,1", "ERROR:a")
  }

  /** Create a new list by applying a function to all elements of a list. */
  case object Map extends Word {

    override def name: String = "map"

    override def matches(stack: List[Any]): Boolean = stack match {
      case (_: List[?]) :: (_: List[?]) :: _ => true
      case _                                 => false
    }

    override def execute(context: Context): Context = {
      context.stack match {
        case (f: List[?]) :: (vs: List[?]) :: stack =>
          val init = context.copy(stack = stack)
          val res = vs.foldLeft(List.empty[Any] -> init) {
            case ((rs, c), v) =>
              val rc =
                c.interpreter.executeProgram(f, c.copy(stack = v :: c.stack), unfreeze = false)
              (rc.stack.head :: rs) -> rc.copy(stack = rc.stack.tail)
          }
          res._2.copy(stack = res._1.reverse :: res._2.stack)
        case _ => invalidStack
      }
    }

    override def summary: String =
      """
        |Create a new list by applying a function to all elements of a list.
      """.stripMargin.trim

    override def signature: String = "items:List f:List -- List(f(items[0]) ... f(items[N]))"

    override def examples: List[String] = List("(,a%s,b%s,),(,(,.netflix.com,),:format,)")
  }

  /** Drop the top N items from the stack. */
  case object NDrop extends SimpleWord {

    override def name: String = "ndrop"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case IntType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case IntType(n) :: vs => vs.drop(n)
    }

    override def summary: String =
      """
        |Remove the top N items on the stack.
        |
        |Since: 1.5.0
      """.stripMargin.trim

    override def signature: String = "aN ... a0 N -- aN"

    override def examples: List[String] = List("a,0", "a,b,c,2", "a,b,c,4", "ERROR:")
  }

  /** Create a list with the top N items from the stack. */
  case object NList extends SimpleWord {

    override def name: String = "nlist"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case IntType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case IntType(n) :: vs => vs.take(n).reverse :: vs.drop(n)
    }

    override def summary: String =
      """
        |Create a list with the top N items on the stack.
        |
        |Since: 1.5.0
      """.stripMargin.trim

    override def signature: String = "aN ... a0 N -- aN List(aN-1 ... a0)"

    override def examples: List[String] = List("a,0", "a,b,c,2", "a,b,c,4", "ERROR:")
  }

  /** Copy the item in the second position on the stack to the top. */
  case object Over extends SimpleWord {

    override def name: String = "over"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Any) :: (_: Any) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case s @ _ :: v :: _ => v :: s
    }

    override def summary: String =
      """
        |Copy the item in the second position on the stack to the top.
      """.stripMargin.trim

    override def signature: String = "a b -- a b a"

    override def examples: List[String] = List("a,b")
  }

  /** Rotate an item in the stack and put it on the top. */
  case object Roll extends SimpleWord {

    override def name: String = "roll"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case IntType(_) :: vs => vs.nonEmpty
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case IntType(n) :: vs =>
        if (n == 0) vs else vs(n) :: (vs.take(n) ::: vs.drop(n + 1))
    }

    override def summary: String =
      """
        |Rotate an item in the stack and put it on the top.
        |
        |Since: 1.5.0
      """.stripMargin.trim

    override def signature: String = "aN ... a0 N -- aN-1 ... a0 aN"

    override def examples: List[String] = List("a,0", "a,b,0", "a,b,1", "ERROR:a")
  }

  /** Rotate the stack so that the item at the bottom is now at the top. */
  case object Rot extends SimpleWord {

    override def name: String = "rot"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case vs => vs.nonEmpty }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case s => s.last :: s.take(s.size - 1)
    }

    override def summary: String =
      """
        |Rotate the stack so that the item at the bottom is now at the top.
      """.stripMargin.trim

    override def signature: String = "a ... b -- ... b a"

    override def examples: List[String] = List("a,b,c,d")
  }

  /** Rotate the stack so that the item at the top is now at the bottom. */
  case object ReverseRot extends SimpleWord {

    override def name: String = "-rot"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case vs => vs.nonEmpty }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case v :: vs => vs ::: List(v)
    }

    override def summary: String =
      """
        |Rotate the stack so that the item at the top is now at the bottom.
      """.stripMargin.trim

    override def signature: String = "* a b -- b * a"

    override def examples: List[String] = List("a,b,c,d")
  }

  /** Set the value of a variable. */
  case object Set extends Word {

    override def name: String = "set"

    override def matches(stack: List[Any]): Boolean = stack match {
      case (_: Any) :: (_: String) :: _ => true
      case _                            => false
    }

    override def execute(context: Context): Context = {
      context.stack match {
        case (v: Any) :: (k: String) :: vs =>
          context.copy(stack = vs, variables = context.variables + (k -> v))
        case _ => invalidStack
      }
    }

    override def summary: String =
      """
        |Set the value of a variable.
      """.stripMargin.trim

    override def signature: String = "k v -- "

    override def examples: List[String] = List("k,v")
  }

  /** Swap the top two items on the stack. */
  case object Swap extends SimpleWord {

    override def name: String = "swap"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Any) :: (_: Any) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (b: Any) :: (a: Any) :: vs => a :: b :: vs
    }

    override def summary: String =
      """
        |Swap the top two items on the stack.
      """.stripMargin.trim

    override def signature: String = "a b -- b a"

    override def examples: List[String] = List("a,b")
  }

  /** Pop all items off the stack and push them as a list. */
  case object ToList extends Word {

    override def name: String = "list"

    override def matches(stack: List[Any]): Boolean = true

    override def execute(context: Context): Context = {
      context.copy(stack = List(context.stack))
    }

    override def summary: String =
      """
        |Pop all items off the stack and push them as a list.
      """.stripMargin.trim

    override def signature: String = "* -- List(*)"

    override def examples: List[String] = List("a,b", "")
  }
}
