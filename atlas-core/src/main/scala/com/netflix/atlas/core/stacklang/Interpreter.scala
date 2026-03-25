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

import com.netflix.atlas.core.stacklang.ast.*
import com.netflix.atlas.core.util.Features
import com.netflix.atlas.core.util.Strings

/**
  * Interpreter for stack expressions.
  *
  * @param vocabulary
  *     Set of supported words. If multiple words have the same name, then the first one that matches
  *     with the current stack will get used.
  */
case class Interpreter(vocabulary: List[Word]) {

  import com.netflix.atlas.core.stacklang.Interpreter.*

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
          case t         => Step(tokens, context.copy(stack = unescape(t) :: context.stack))
        }
      case Nil => s
    }
  }

  @scala.annotation.tailrec
  private def execute(s: Step): Context = {
    if (s.context.callDepth > 10) {
      // Prevent infinite loops. Operations like `:each` and `:map` to traverse a list are
      // finite and will increase the depth by 1. The max call depth of 10 is arbitrary, but
      // should be more than enough for legitimate use-cases. Testing 1M actual expressions, 3
      // was the highest depth seen.
      throw new IllegalStateException(s"looping detected")
    }
    if (s.program.isEmpty) s.context else execute(nextStep(s))
  }

  final def executeProgram(
    program: List[Any],
    context: Context,
    unfreeze: Boolean = true
  ): Context = {
    val result = execute(Step(program, context.incrementCallDepth)).decrementCallDepth
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
    debug(program, Context(this, Nil, Map.empty, features = Features.UNSTABLE))
  }

  final def debug(program: String): List[Step] = {
    debug(splitAndTrim(program))
  }

  /**
    * Build a syntax tree for the given expression string. Unlike `execute`, this method
    * recovers from errors and continues processing, collecting diagnostics for each
    * problem encountered. The resulting tree contains position information for all tokens,
    * resolved word references, and the stack state at each point.
    */
  final def syntaxTree(
    str: String,
    vars: Map[String, Any] = Map.empty
  ): SyntaxTree = {
    val tokens = Interpreter.tokenize(str)
    val diagnostics = List.newBuilder[Diagnostic]
    var stack: List[Any] = Nil
    var currentVars: Map[String, Any] = vars

    def buildNodes(ts: List[Token]): (List[SyntaxNode], List[Token]) = {
      val nodes = List.newBuilder[SyntaxNode]
      var remaining = ts
      var currentStack = stack

      while (remaining.nonEmpty) {
        remaining.head match {
          case ct: CommentToken =>
            remaining = remaining.tail
            nodes += CommentNode(ct)
          case token: ValueToken =>
            remaining = remaining.tail
            token.value match {
              case "(" =>
                val (children, rest) = buildListChildren(remaining)
                val closeToken = rest.headOption.collect {
                  case vt: ValueToken if vt.value == ")" => vt
                }
                val diag = if (closeToken.isEmpty) {
                  val d = Diagnostic(token.span, "unmatched opening parenthesis", Severity.Error)
                  diagnostics += d
                  Some(d)
                } else None
                // Build the list value from children (strings only, not executed)
                val listValues = children.collect {
                  case LiteralNode(t)       => t.value
                  case WordNode(t, _, _, _) => t.value
                }
                currentStack = listValues :: currentStack
                stack = currentStack
                val node = ListNode(token, children, closeToken, diag)
                nodes += node
                remaining = if (closeToken.isDefined) rest.tail else rest
              case ")" =>
                val d = Diagnostic(token.span, "unmatched closing parenthesis", Severity.Error)
                diagnostics += d
                nodes += LiteralNode(token)
              // Don't update the stack for unmatched close paren
              case v if v.startsWith(":") =>
                val name = v.substring(1)
                val stackBefore = currentStack
                words.get(name) match {
                  case None =>
                    val d = Diagnostic(token.span, s"unknown word ':$name'", Severity.Error)
                    diagnostics += d
                    nodes += WordNode(token, None, stackBefore, Some(d))
                  case Some(ws) =>
                    val matched = ws.find(_.matches(currentStack))
                    val ctx = Context(
                      this,
                      currentStack,
                      currentVars,
                      currentVars,
                      features = Features.UNSTABLE
                    )
                    try {
                      val result = executeFirstMatchingWord(name, ws, ctx)
                      currentStack = result.stack
                      stack = currentStack
                      currentVars = result.variables
                      val diag = matched
                        .flatMap { w =>
                          if (!w.isStable) {
                            Some(
                              Diagnostic(token.span, s":${w.name} is unstable", Severity.Warning)
                            )
                          } else
                            w.deprecated.map { msg =>
                              Diagnostic(
                                token.span,
                                s":${w.name} is deprecated: $msg",
                                Severity.Warning
                              )
                            }
                        }
                        .map { d =>
                          diagnostics += d
                          d
                        }
                      nodes += WordNode(token, matched, stackBefore, diag)
                    } catch {
                      case e: Exception =>
                        // The word matched the stack but execution failed (e.g.
                        // :each body error). Approximate the stack effect to avoid
                        // cascading false diagnostics for subsequent words.
                        matched match {
                          case Some(tw: TypedWord) =>
                            val n = tw.parameters.length
                            currentStack = currentStack.drop(n)
                            tw.outputs.foreach { dt =>
                              currentStack = s"<${tw.name}:${dt.name}>" :: currentStack
                            }
                            stack = currentStack
                            nodes += WordNode(token, Some(tw), stackBefore, None)
                          case Some(w) =>
                            // Non-TypedWord that matches — record as matched,
                            // leave the stack unchanged (conservative).
                            nodes += WordNode(token, Some(w), stackBefore, None)
                          case None =>
                            val d = Diagnostic(token.span, e.getMessage, Severity.Error)
                            diagnostics += d
                            nodes += WordNode(token, None, stackBefore, Some(d))
                        }
                    }
                }
              case v =>
                if (v.startsWith(";")) {
                  val name = v.substring(1)
                  if (words.contains(name)) {
                    val d = Diagnostic(
                      token.span,
                      s"did you mean ':$name'? (semicolon instead of colon)",
                      Severity.Warning
                    )
                    diagnostics += d
                  }
                }
                currentStack = Interpreter.unescape(v) :: currentStack
                stack = currentStack
                nodes += LiteralNode(token)
            }
        }
      }
      (nodes.result(), remaining)
    }

    def buildListChildren(ts: List[Token]): (List[SyntaxNode], List[Token]) = {
      val nodes = List.newBuilder[SyntaxNode]
      var remaining = ts

      while (remaining.nonEmpty) {
        remaining.head match {
          case ct: CommentToken =>
            remaining = remaining.tail
            nodes += CommentNode(ct)
          case token: ValueToken =>
            token.value match {
              case ")" =>
                return (nodes.result(), remaining)
              case "(" =>
                remaining = remaining.tail
                val (children, rest) = buildListChildren(remaining)
                val closeToken = rest.headOption.collect {
                  case vt: ValueToken if vt.value == ")" => vt
                }
                val node = ListNode(token, children, closeToken, None)
                nodes += node
                remaining = if (closeToken.isDefined) rest.tail else rest
              case _ =>
                remaining = remaining.tail
                nodes += LiteralNode(token)
            }
        }
      }
      (nodes.result(), remaining)
    }

    val (nodes, _) = buildNodes(tokens)
    SyntaxTree(str, nodes, diagnostics.result(), stack)
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

  case class WordToken(name: String)

  case object IsWord {

    def unapply(v: String): Option[String] = if (v.startsWith(":")) Some(v.substring(1)) else None
  }

  /**
    * List classes show simple names like "Nil$" and "colon$colon$" that are confusing to most
    * looking at the output. This tries to detect the list and use the simple name "List".
    */
  private def getTypeName(v: Any): String = v match {
    case _: List[?] => "List"
    case _          => v.getClass.getSimpleName
  }

  def typeSummary(stack: List[Any]): String = {
    stack.reverse.map(getTypeName).mkString("[", ",", "]")
  }

  def toString(items: Any*): String = toStringImpl(items)

  def toString(stack: List[Any]): String = toStringImpl(stack.reverse)

  private def toStringImpl(items: Seq[Any]): String = {
    val builder = new java.lang.StringBuilder()
    appendValues(builder, items)
    builder.toString
  }

  def append(builder: java.lang.StringBuilder, items: Any*): Unit = {
    appendValues(builder, items)
  }

  private def appendValues(builder: java.lang.StringBuilder, vs: Seq[Any]): Unit = {
    val it = vs.iterator
    if (it.hasNext) {
      appendValue(builder, it.next())
      while (it.hasNext) {
        builder.append(',')
        appendValue(builder, it.next())
      }
    }
  }

  private def appendValue(builder: java.lang.StringBuilder, value: Any): Unit = {
    value match {
      case vs: Seq[?]   => appendSeq(builder, vs)
      case v: String    => escape(builder, v)
      case v: WordToken => builder.append(v.name)
      case v: StackItem => v.append(builder)
      case v            => builder.append(v)
    }
  }

  private def appendSeq(builder: java.lang.StringBuilder, vs: Seq[Any]): Unit = {
    builder.append('(')
    vs.foreach { v =>
      builder.append(',')
      appendValue(builder, v)
    }
    builder.append(",)")
  }

  /**
    * Helper for efficiently splitting the input string. See StringSplit benchmark for
    * comparison with more naive approach. The string split method optimizes simple cases
    * with a single character so that it does not require compiling the regex. This method
    * uses a single character and does a simple `trim` operation to cleanup the whitespace.
    */
  def splitAndTrim(str: String): List[String] = {
    val parts = stripComments(str).split(",")
    val builder = List.newBuilder[String]
    var i = 0
    while (i < parts.length) {
      val tmp = parts(i).trim
      if (tmp.nonEmpty)
        builder += tmp
      i += 1
    }
    builder.result()
  }

  /**
    * Check if a character is special in the string.
    */
  private def isSpecial(c: Int): Boolean = {
    c == ',' || c == ':' || Character.isWhitespace(c)
  }

  /**
    * Check if a character is special in the middle of a string, with special handling
    * for spaces. For spaces, only escape if they are at the leading or trailing edge
    * where they would be removed by trim. This makes expressions with spaces in the
    * middle more user-friendly.
    */
  private def isSpecialForMiddle(c: Int): Boolean = {
    // In the middle of the string, spaces are not special, but other characters are
    c == ',' || c == ':' || (Character.isWhitespace(c) && c != ' ')
  }

  private def indexOfNonWhitespace(str: String): Int = {
    var i = 0
    while (i < str.length) {
      if (!Character.isWhitespace(str.charAt(i)))
        return i
      i += 1
    }
    -1
  }

  private def lastIndexOfNonWhitespace(str: String): Int = {
    var i = str.length - 1
    while (i >= 0) {
      if (!Character.isWhitespace(str.charAt(i)))
        return i
      i -= 1
    }
    -1
  }

  /** Escape special characters in the expression. */
  def escape(str: String): String = {
    // Parenthesis only matter if they are the only part of the token for constructing
    // lists. Only escape if using as a literal in that context. For things like regex
    // escaping makes the expression much harder to read.
    val escaped = str match {
      case "(" => "\\u0028"
      case ")" => "\\u0029"
      case s =>
        val f = indexOfNonWhitespace(s)
        val l = lastIndexOfNonWhitespace(s)
        if (f >= 0 && l >= 0) {
          val prefix = Strings.escape(s.substring(0, f), isSpecial)
          val middle = Strings.escape(s.substring(f, l + 1), isSpecialForMiddle)
          val suffix = Strings.escape(s.substring(l + 1), isSpecial)
          s"$prefix$middle$suffix"
        } else {
          Strings.escape(s, isSpecial)
        }
    }
    escapeCommentStart(escaped)
  }

  /** Escape special characters in the expression. */
  def escape(builder: java.lang.StringBuilder, str: String): Unit = {
    // Parenthesis only matter if they are the only part of the token for constructing
    // lists. Only escape if using as a literal in that context. For things like regex
    // escaping makes the expression much harder to read.
    val start = builder.length
    str match {
      case "(" => builder.append("\\u0028")
      case ")" => builder.append("\\u0029")
      case s =>
        val f = indexOfNonWhitespace(s)
        val l = lastIndexOfNonWhitespace(s)
        if (f >= 0 && l >= 0) {
          Strings.escape(builder, s.substring(0, f), isSpecial)
          Strings.escape(builder, s.substring(f, l + 1), isSpecialForMiddle)
          Strings.escape(builder, s.substring(l + 1), isSpecial)
        } else {
          Strings.escape(builder, s, isSpecial)
        }
    }
    escapeCommentStart(builder, start)
  }

  /** Escape comment start sequences so they are not interpreted as comment delimiters. */
  private def escapeCommentStart(str: String): String = {
    str.replace("/*", "\\u002f*")
  }

  /** Escape comment start sequences so they are not interpreted as comment delimiters. */
  private def escapeCommentStart(builder: java.lang.StringBuilder, start: Int): Unit = {
    var i = start
    while (i < builder.length - 1) {
      if (builder.charAt(i) == '/' && builder.charAt(i + 1) == '*') {
        builder.replace(i, i + 1, "\\u002f")
        i += 7 // skip past \u002F*
      } else {
        i += 1
      }
    }
  }

  /** Unescape unicode characters in the expression. */
  def unescape(str: String): String = {
    Strings.unescape(str)
  }

  /** Unescape unicode characters in the expression. */
  def unescape(value: Any): Any = value match {
    case s: String => Strings.unescape(s)
    case v         => v
  }

  /**
    * Split the input string on commas and trim whitespace, returning tokens with their
    * character positions in the original string. Comments are returned as `CommentToken`
    * and regular values as `ValueToken`. When a comment is embedded inside a value
    * (e.g., `:d&#47;*c*&#47;up`), the value fragments are tracked in the `spans` list
    * so that each fragment can be highlighted independently.
    */
  def tokenize(str: String): List[Token] = {
    val n = str.length
    val result = List.newBuilder[Token]

    // First pass: find comma positions that are outside comments
    val commaPositions = List.newBuilder[Int]
    var depth = 0
    var i = 0
    while (i < n) {
      val c = str.charAt(i)
      if (c == '/' && i + 1 < n && str.charAt(i + 1) == '*') {
        depth += 1
        i += 2
      } else if (c == '*' && i + 1 < n && str.charAt(i + 1) == '/') {
        depth -= 1
        if (depth < 0)
          throw new IllegalStateException("unclosed comment")
        i += 2
      } else {
        if (c == ',' && depth == 0)
          commaPositions += i
        i += 1
      }
    }
    if (depth > 0)
      throw new IllegalStateException("unclosed comment")

    // Second pass: process each comma-delimited segment
    val commas = commaPositions.result()
    val boundaries = -1 :: commas ::: List(n)

    var bi = 0
    while (bi < boundaries.size - 1) {
      val segStart = boundaries(bi) + 1
      val segEnd = boundaries(bi + 1)
      emitSegmentTokens(str, segStart, segEnd, result)
      bi += 1
    }

    result.result()
  }

  /**
    * Process a single comma-delimited segment, emitting `CommentToken`s and at most one
    * `ValueToken`. The value token's text is the concatenation of all non-comment,
    * non-whitespace-trimmed fragments, and its `spans` list tracks each fragment's position.
    */
  private def emitSegmentTokens(
    str: String,
    segStart: Int,
    segEnd: Int,
    result: collection.mutable.Builder[Token, List[Token]]
  ): Unit = {
    val valueSpans = List.newBuilder[Span]
    val valueText = new java.lang.StringBuilder()
    val comments = List.newBuilder[CommentToken]
    var i = segStart

    while (i < segEnd) {
      val c = str.charAt(i)
      if (c == '/' && i + 1 < segEnd && str.charAt(i + 1) == '*') {
        // Parse comment
        val commentStart = i
        var depth = 1
        i += 2
        while (i < str.length && depth > 0) {
          if (str.charAt(i) == '/' && i + 1 < str.length && str.charAt(i + 1) == '*') {
            depth += 1
            i += 2
          } else if (str.charAt(i) == '*' && i + 1 < str.length && str.charAt(i + 1) == '/') {
            depth -= 1
            i += 2
          } else {
            i += 1
          }
        }
        comments += CommentToken(str.substring(commentStart, i), Span(commentStart, i))
      } else {
        // Non-comment text: collect until next comment start or segment end
        val fragStart = i
        while (
          i < segEnd && !(str.charAt(i) == '/' && i + 1 < segEnd && str.charAt(i + 1) == '*')
        ) {
          i += 1
        }
        // Trim whitespace from this fragment
        var s = fragStart
        var e = i
        while (s < e && Character.isWhitespace(str.charAt(s))) s += 1
        while (e > s && Character.isWhitespace(str.charAt(e - 1))) e -= 1
        if (s < e) {
          valueSpans += Span(s, e)
          valueText.append(str, s, e)
        }
      }
    }

    // Emit tokens in source order
    val spans = valueSpans.result()
    val commentList = comments.result()

    // Collect all tokens with their start positions for ordering
    val items = List.newBuilder[(Int, Token)]
    if (spans.nonEmpty) {
      items += (spans.head.start -> ValueToken(valueText.toString, spans))
    }
    commentList.foreach { c =>
      items += (c.span.start -> c)
    }
    items.result().sortBy(_._1).foreach { case (_, token) => result += token }
  }

  /**
    * Remove comment strings from the expression. Comments have the form `/* ... */`
    * and can be nested for example `/* outer /* /*nested*/ text */ text */`.
    */
  def stripComments(str: String): String = {
    val pos = str.indexOf("/*")
    if (pos < 0) {
      if (str.contains("*/"))
        throw new IllegalStateException("unclosed comment")
      str
    } else {
      stripCommentsImpl(str, pos)
    }
  }

  private def stripCommentsImpl(str: String, pos: Int): String = {
    val n = str.length
    val builder = new java.lang.StringBuilder(n)
    builder.append(str, 0, pos)
    var depth = 1
    var i = pos + 2
    while (i < n) {
      val c = str.charAt(i)
      if (c == '/' && i + 1 < n && str.charAt(i + 1) == '*') {
        depth += 1
        i += 2
      } else if (c == '*' && i + 1 < n && str.charAt(i + 1) == '/') {
        depth -= 1
        if (depth < 0) {
          throw new IllegalStateException("unclosed comment")
        }
        i += 2
      } else {
        if (depth == 0) {
          builder.append(c)
        }
        i += 1
      }
    }
    if (depth > 0) {
      throw new IllegalStateException("unclosed comment")
    }
    builder.toString
  }
}
