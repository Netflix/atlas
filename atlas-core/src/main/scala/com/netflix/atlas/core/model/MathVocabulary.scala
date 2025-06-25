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

import java.time.ZoneId
import java.time.ZoneOffset
import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.model.MathExpr.AggrMathExpr
import com.netflix.atlas.core.model.MathExpr.NamedRewrite
import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.StandardVocabulary.Macro
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.spectator.api.histogram.PercentileBuckets

object MathVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.ModelExtractors.*
  import com.netflix.atlas.core.stacklang.Extractors.*

  val name: String = "math"

  val dependsOn: List[Vocabulary] = List(DataVocabulary)

  val words: List[Word] = List(
    As,
    GroupBy,
    Const,
    Pi,
    Random,
    SeededRandom,
    Time,
    TimeSpan,
    CommonGroupBy,
    NamedRewrite,
    ClampMin,
    ClampMax,
    Abs,
    Negate,
    Sine,
    Sqrt,
    PerStep,
    Add,
    Subtract,
    Multiply,
    Divide,
    Power,
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    LessThanEqual,
    FAdd,
    FSubtract,
    FMultiply,
    FDivide,
    And,
    Or,
    Sum,
    Count,
    Min,
    Max,
    Percentiles,
    SampleCount,
    Macro(
      "avg",
      List(
        ":dup",
        ":dup",
        ":sum",
        ":swap",
        ":count",
        ":div",
        "avg",
        ":named-rewrite"
      ),
      List("name,sps,:eq,(,nf.cluster,),:by", "name,sps,:eq,1h,:offset")
    ),
    Macro(
      "stddev",
      List(
        // Copy of base query
        ":dup",
        // If the aggregate function is not explicit, then we need to force
        // the conversion. Using `fadd` avoids conversion of `NaN` values to
        // zero.
        "0",
        ":fadd",
        ":dup",
        // N
        ":count",
        // sum(x^2)
        ":over",
        ":dup",
        ":mul",
        ":sum",
        // N * sum(x^2)
        ":mul",
        // sum(x)
        ":over",
        ":sum",
        // sum(x)^2
        ":dup",
        ":mul",
        // N * sum(x^2) - sum(x)^2
        ":sub",
        // N^2
        ":swap",
        ":count",
        ":dup",
        ":mul",
        // v = (N * sum(x^2) - sum(x)^2) / N^2
        ":div",
        // stddev = sqrt(v)
        ":sqrt",
        // Avoid expansion when displayed
        "stddev",
        ":named-rewrite"
      ),
      List("name,sps,:eq,(,nf.cluster,),:by")
    ),
    Macro(
      "pct",
      List(
        ":dup",
        ":dup",
        ":sum",
        ":div",
        "100",
        ":mul",
        "pct",
        ":named-rewrite"
      ),
      List("name,sps,:eq,(,nf.cluster,),:by")
    ),
    Macro(
      "dist-avg",
      List(
        ":dup",
        "statistic",
        "(",
        "totalTime",
        "totalAmount",
        ")",
        ":in",
        ":sum",
        "statistic",
        "count",
        ":eq",
        ":sum",
        ":div",
        ":swap",
        ":cq",
        "dist-avg",
        ":named-rewrite"
      ),
      List("name,playback.startLatency,:eq")
    ),
    Macro(
      "dist-max",
      List(
        ":dup",
        "statistic",
        "max",
        ":eq",
        ":max",
        ":swap",
        ":cq",
        "dist-max",
        ":named-rewrite"
      ),
      List("name,playback.startLatency,:eq")
    ),
    Macro(
      "dist-stddev",
      List(
        ":dup",
        // N
        "statistic",
        "count",
        ":eq",
        ":sum",
        // sum(x^2)
        "statistic",
        "totalOfSquares",
        ":eq",
        ":sum",
        // N * sum(x^2)
        ":mul",
        // sum(x)
        "statistic",
        "(",
        "totalAmount",
        "totalTime",
        ")",
        ":in",
        ":sum",
        // sum(x)^2
        ":dup",
        ":mul",
        // N * sum(x^2) - sum(x)^2
        ":sub",
        // N^2
        "statistic",
        "count",
        ":eq",
        ":sum",
        ":dup",
        ":mul",
        // v = (N * sum(x^2) - sum(x)^2) / N^2
        ":div",
        // stddev = sqrt(v)
        ":sqrt",
        // Swap and use :cq to apply a common query
        ":swap",
        ":cq",
        // Avoid expansion when displayed
        "dist-stddev",
        ":named-rewrite"
      ),
      List("name,playback.startLatency,:eq")
    ),
    Macro("median", List("(", "50", ")", ":percentiles"), List("name,requestLatency,:eq")),
    Macro("cos", List(":pi", "2", ":div", ":swap", ":sub", ":sin")),
    Macro("tan", List(":dup", ":sin", ":swap", ":cos", ":div")),
    Macro("cot", List(":dup", ":cos", ":swap", ":sin", ":div")),
    Macro("sec", List("1", ":swap", ":cos", ":div")),
    Macro("csc", List("1", ":swap", ":sin", ":div"))
  )

  case object As extends SimpleWord {

    override def name: String = "as"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: (_: String) :: TimeSeriesType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (replacement: String) :: (original: String) :: TimeSeriesType(t) :: stack =>
        MathExpr.As(t, original, replacement) :: stack
    }

    override def summary: String =
      """
        |Map a tag key name to an alternate name.
      """.stripMargin.trim

    override def signature: String = {
      "TimeSeriesExpr original:String replacement:String -- TimeSeriesExpr"
    }

    override def examples: List[String] = List("name,sps,:eq,(,nf.cluster,),:by,nf.cluster,c")
  }

  case object GroupBy extends SimpleWord {

    override def name: String = "by"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case StringListType(_) :: (t: AggrMathExpr) :: _ if t.expr.isGrouped =>
        // Multi-level group by with an explicit aggregate specified
        true
      case StringListType(_) :: TimeSeriesType(t) :: _ if t.isGrouped =>
        // Multi-level group by with an implicit aggregate of :sum
        true
      case StringListType(_) :: TimeSeriesType(_) :: _ =>
        // Default data or math aggregate group by applied across math operations
        true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case StringListType(keys) :: (t: AggrMathExpr) :: stack if t.expr.isGrouped =>
        // Multi-level group by with an explicit aggregate specified
        MathExpr.GroupBy(t, keys) :: stack
      case StringListType(keys) :: TimeSeriesType(t) :: stack if t.isGrouped =>
        // Multi-level group by with an implicit aggregate of :sum
        MathExpr.GroupBy(MathExpr.Sum(t), keys) :: stack
      case StringListType(keys) :: TimeSeriesType(t) :: stack =>
        // Default data group by applied across math operations
        val f = t.rewrite {
          case nr: NamedRewrite                      => nr.groupBy(keys)
          case af: AggregateFunction                 => DataExpr.GroupBy(af, keys)
          case af: AggrMathExpr if af.expr.isGrouped => MathExpr.GroupBy(af, keys)
        }
        f :: stack
    }

    override def summary: String =
      """
        |Apply a common group by to all aggregation functions in the expression.
      """.stripMargin.trim

    override def signature: String = "TimeSeriesExpr keys:List -- TimeSeriesExpr"

    override def examples: List[String] = List("name,sps,:eq,:avg,(,nf.cluster,)")
  }

  case object Const extends SimpleWord {

    override def name: String = "const"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (v: String) :: stack => MathExpr.Constant(v.toDouble) :: stack
    }

    override def summary: String =
      """
        |Generates a line where each datapoint is a constant value.
      """.stripMargin.trim

    override def signature: String = "Double -- TimeSeriesExpr"

    override def examples: List[String] = List("42")
  }

  case object Pi extends SimpleWord {

    override def name: String = "pi"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case stack => MathExpr.Constant(Math.PI) :: stack
    }

    override def summary: String =
      """
        |Generates a line where each datapoint has the value of the mathematical constant π.
      """.stripMargin.trim

    override def signature: String = " -- TimeSeriesExpr"

    override def examples: List[String] = Nil
  }

  case object Random extends SimpleWord {

    override def name: String = "random"

    protected def matcher: PartialFunction[List[Any], Boolean] = { case _ => true }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case s => MathExpr.Random :: s
    }

    override def summary: String =
      """
        |Generate a time series that appears to be random noise for the purposes of
        |experimentation and generating sample data. To ensure that the line is deterministic
        |and reproducible it actually is based on a hash of the timestamp. Each datapoint is a
        |value between 0.0 and 1.0.
      """.stripMargin.trim

    override def signature: String = " -- TimeSeriesExpr"

    override def examples: List[String] = List("")
  }

  case object SeededRandom extends SimpleWord {

    override def name: String = "srandom"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case IntType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case IntType(seed) :: s => MathExpr.SeededRandom(seed) :: s
    }

    override def summary: String =
      """
        |Generate a time series that appears to be random noise for the purposes of
        |experimentation and generating sample data. To ensure that the line is deterministic
        |and reproducible it actually is based on a hash of the timestamp. The seed value is
        |used to vary the values for the purposes of creating mulitple different sample lines.
        |Each datapoint is a value between 0.0 and 1.0.
      """.stripMargin.trim

    override def signature: String = "seed:Int -- TimeSeriesExpr"

    override def examples: List[String] = List("42")
  }

  case object Time extends SimpleWord {

    override def name: String = "time"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (v: String) :: stack => MathExpr.Time(v) :: stack
    }

    override def summary: String =
      """
        |Generates a line based on the current time. Supported modes are secondOfMinute,
        |secondOfDay, minuteOfHour, minuteOfDay, hourOfDay, dayOfWeek, dayOfMonth, dayOfYear,
        |monthOfYear, yearOfCentury, yearOfEra, seconds (since epoch), or days (since epoch). The
        |mode can also be a value of the enum
        |[ChronoField](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoField.html).
      """.stripMargin.trim

    override def signature: String = "String -- TimeSeriesExpr"

    override def examples: List[String] = List("hourOfDay", "HOUR_OF_DAY")
  }

  case object TimeSpan extends Word {

    override def name: String = "time-span"

    def matches(stack: List[Any]): Boolean = stack match {
      case (_: String) :: (_: String) :: _ => true
      case _                               => false
    }

    def execute(context: Context): Context = {
      val zone = context.variables.get("tz") match {
        case Some(z: String) => ZoneId.of(z)
        case Some(z: ZoneId) => z
        case _               => ZoneOffset.UTC
      }
      val newStack = context.stack match {
        case (e: String) :: (s: String) :: stack => MathExpr.TimeSpan(s, e, zone) :: stack
        case _                                   => invalidStack
      }
      context.copy(stack = newStack)
    }

    override def summary: String =
      """
        |Generates a signal line based on the specified time range. The line will be 1
        |within the range and 0 for all other times. The format of the start and end times
        |is the same as the start and end [time parameters](Time-Parameters) on the Graph
        |API. If the time zone is not explicitly specified, then the value from the `tz`
        |variable will get used. The default value for the `tz` variable is the primary
        |time zone used for the graph.
        |
        |The following named times are supported for time spans:
        |
        || Name     | Description                                                 |
        ||----------|-------------------------------------------------------------|
        || gs       | Graph start time.                                           |
        || ge       | Graph end time.                                             |
        || s        | Start time for the span, can only be used for the end time. |
        || e        | End time for the span, can only be used for the start time. |
        || now      | Current time.                                               |
        || epoch    | January 1, 1970 UTC.                                        |
        |
        |Since: 1.6
      """.stripMargin.trim

    override def signature: String = "s:String e:String -- TimeSeriesExpr"

    override def examples: List[String] = List("e-30m,ge", "2014-02-20T13:00,s%2B30m")
  }

  case object CommonGroupBy extends SimpleWord {

    override def name: String = "cg"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case StringListType(_) :: (t: AggrMathExpr) :: _ if t.expr.isGrouped =>
        // Multi-level group by with an explicit aggregate specified
        true
      case StringListType(_) :: TimeSeriesType(t) :: _ if t.isGrouped =>
        // Multi-level group by with an implicit aggregate of :sum
        true
      case StringListType(_) :: TimeSeriesType(_) :: _ =>
        // Default data or math aggregate group by applied across math operations
        true
      case StringListType(_) :: PresentationType(_) :: _ =>
        // Handle :cg after presentation operators
        true
    }

    private def mergeKeys(ks1: List[String], ks2: List[String]): List[String] = {
      val set = ks1.toSet
      ks1 ::: ks2.filterNot(set.contains)
    }

    private def addCommonKeys(expr: TimeSeriesExpr, keys: List[String]): TimeSeriesExpr = {
      val newExpr = expr.rewrite {
        case nr @ MathExpr.NamedRewrite(_, _: Query, _, e, _, _) if e.isGrouped =>
          nr.copy(evalExpr = addCommonKeys(e, keys))
        case nr @ MathExpr.NamedRewrite(_, _: Query, _, _, _, _) =>
          nr.groupBy(keys)
        case af: AggregateFunction =>
          DataExpr.GroupBy(af, keys)
        case af: MathExpr.AggrMathExpr =>
          val e = addCommonKeys(af.expr, keys)
          MathExpr.GroupBy(copy(af, e), keys)
        case e: DataExpr.GroupBy =>
          e.copy(keys = mergeKeys(e.keys, keys))
        case e: MathExpr.GroupBy =>
          val af = copy(e.expr, addCommonKeys(e.expr.expr, keys))
          MathExpr.GroupBy(af, mergeKeys(e.keys, keys))
      }
      newExpr.asInstanceOf[TimeSeriesExpr]
    }

    private def copy(aggr: AggrMathExpr, expr: TimeSeriesExpr): AggrMathExpr = {
      aggr match {
        case af @ MathExpr.Count(_) => af.copy(expr = expr)
        case af @ MathExpr.Max(_)   => af.copy(expr = expr)
        case af @ MathExpr.Min(_)   => af.copy(expr = expr)
        case af @ MathExpr.Sum(_)   => af.copy(expr = expr)
      }
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case StringListType(keys) :: TimeSeriesType(t) :: stack =>
        addCommonKeys(t, keys) :: stack
      case StringListType(keys) :: PresentationType(e) :: stack =>
        StyleExpr(addCommonKeys(e.expr, keys), e.settings) :: stack
    }

    override def summary: String =
      """
        |Recursively add a list of keys to group by expressions
      """.stripMargin.trim

    override def signature: String = "Expr keys:List -- Expr"

    override def examples: List[String] =
      List(
        "name,sps,:eq,(,nf.region,)",
        "name,bar,:eq,(,nf.node,),:by,(,nf.region,)"
      )
  }

  case object NamedRewrite extends Word {

    override def name: String = "named-rewrite"

    def matches(stack: List[Any]): Boolean = {
      if (matcher.isDefinedAt(stack)) matcher(stack) else false
    }

    def execute(context: Context): Context = {
      val pf = executor(Context(context.interpreter, Nil, Map.empty))
      if (pf.isDefinedAt(context.stack))
        context.copy(stack = pf(context.stack))
      else
        invalidStack
    }

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: TimeSeriesType(_) :: TimeSeriesType(_) :: _ => true
      case (_: String) :: TimeSeriesType(_) :: (_: StyleExpr) :: _    => true
      case (_: String) :: (_: StyleExpr) :: (_: StyleExpr) :: _       => true
    }

    protected def executor(context: Context): PartialFunction[List[Any], List[Any]] = {
      case (n: String) :: (rw: StyleExpr) :: (orig: StyleExpr) :: stack =>
        // If the original and rewrite have presentation settings, then ignore
        // on the rewrite and carry forward presentation.
        val nrw = MathExpr.NamedRewrite(n, orig.expr, Nil, rw.expr, context)
        orig.copy(expr = nrw) :: stack
      case (n: String) :: TimeSeriesType(rw) :: (orig: StyleExpr) :: stack =>
        // If the original has presentation settings, apply the rewrite to the
        // underlying expression and carry forward the presentation.
        val nrw = MathExpr.NamedRewrite(n, orig.expr, Nil, rw, context)
        orig.copy(expr = nrw) :: stack
      case (n: String) :: TimeSeriesType(rw) :: (orig: Expr) :: stack =>
        // If the original is already an expr type, e.g. a Query, then we should
        // preserve it without modification. So we first match for Expr.
        MathExpr.NamedRewrite(n, orig, Nil, rw, context) :: stack
      case (n: String) :: TimeSeriesType(rw) :: TimeSeriesType(orig) :: stack =>
        // This is a more general match that will coerce the original into a
        // TimeSeriesExpr if it is not one already, e.g., a constant.
        MathExpr.NamedRewrite(n, orig, Nil, rw, context) :: stack
    }

    override def summary: String =
      """
        |Internal operation used by some macros to provide a more user friendly display
        |expression. The expanded version will get used for evaluation, but if a new expression
        |is generated from the parsed expression tree it will use the original version
        |along with the named of the macro.
      """.stripMargin.trim

    override def signature: String = {
      "original:TimeSeriesExpr rewritten:TimeSeriesExpr name:String -- TimeSeriesExpr"
    }

    override def examples: List[String] = Nil
  }

  case object ClampMin extends SimpleWord {

    override def name: String = "clamp-min"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case DoubleType(_) :: TimeSeriesType(_) :: _ => true
      case DoubleType(_) :: (_: StyleExpr) :: _    => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case DoubleType(mn) :: TimeSeriesType(t) :: stack => MathExpr.ClampMin(t, mn) :: stack
      case DoubleType(mn) :: (t: StyleExpr) :: stack =>
        t.copy(expr = MathExpr.ClampMin(t.expr, mn)) :: stack
    }

    override def summary: String =
      """
        |Restricts the minimum value of the output time series to the specified value. Values
        |from the input time series that are greater than or equal to the minimum will not be
        |changed. A common use-case is to allow for auto-scaled axis up to a specified bound.
        |For more details see [:clamp-max](math-clamp‐max).
      """.stripMargin.trim

    override def signature: String = "TimeSeriesExpr Double -- TimeSeriesExpr"

    override def examples: List[String] = List("name,sps,:eq,:sum,200e3")
  }

  case object ClampMax extends SimpleWord {

    override def name: String = "clamp-max"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case DoubleType(_) :: TimeSeriesType(_) :: _ => true
      case DoubleType(_) :: (_: StyleExpr) :: _    => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case DoubleType(mx) :: TimeSeriesType(t) :: stack => MathExpr.ClampMax(t, mx) :: stack
      case DoubleType(mx) :: (t: StyleExpr) :: stack =>
        t.copy(expr = MathExpr.ClampMax(t.expr, mx)) :: stack
    }

    override def summary: String =
      """
        |Restricts the maximum value of the output time series to the specified value. Values
        |from the input time series that are less than or equal to the maximum will not be
        |changed.
        |
        |A common use-case is to allow for auto-scaled axis up to a specified bound. The
        |axis parameters for controlling the [axis bounds](Axis-Bounds) have the following
        |limitations:
        |
        |- They apply to everything on the axis and cannot be targeted to a specific line.
        |- Are either absolute or set based on the data. For data with occasional spikes
        |  this can hide important details.
        |
        |Consider the following graph:
        |
        |![Spike](images/clamp-max-spike.png)
        |
        |The spike makes it difficult to make out any detail for other times. One option
        |to handle this is to use an alternate [axis scale](Axis-Scale) such as
        |[logarithmic](Axis-Scale#logarithmic) that gives a higher visual weight to the smaller
        |values. However, it is often easier for a user to reason about a linear scale, in
        |particular, for times when there is no spike in the graph window. If there is a known
        |max reasonable value, then the `:clamp-max` operator can be used to restrict the line
        |if and only if it exceeds the designated max. For example, if we limit the graph above
        |to 25:
        |
        |![Spike with 25,:clamp-max](images/clamp-max-25.png)
      """.stripMargin.trim

    override def signature: String = "TimeSeriesExpr Double -- TimeSeriesExpr"

    override def examples: List[String] = List("name,sps,:eq,:sum,200e3")
  }

  sealed trait UnaryWord extends SimpleWord {

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case TimeSeriesType(_) :: _ => true
      case (_: StyleExpr) :: _    => true
    }

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case TimeSeriesType(t) :: stack => newInstance(t) :: stack
      case (t: StyleExpr) :: stack    => t.copy(expr = newInstance(t.expr)) :: stack
    }

    override def signature: String = "TimeSeriesExpr -- TimeSeriesExpr"

    override def examples: List[String] = List("0", "64", "-64")
  }

  case object Abs extends UnaryWord {

    override def name: String = "abs"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Abs(t)

    override def summary: String =
      """
        |Compute a new time series where each interval has the absolute value of the input time
        |series.
      """.stripMargin.trim
  }

  case object Negate extends UnaryWord {

    override def name: String = "neg"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Negate(t)

    override def summary: String =
      """
        |Compute a new time series where each interval has the negated value of the input time
        |series.
      """.stripMargin.trim
  }

  case object Sine extends UnaryWord {

    override def name: String = "sin"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Sine(t)

    override def summary: String =
      """
        |Compute a new time series where each interval has the sine of the value from the
        |input time series.
      """.stripMargin.trim
  }

  case object Sqrt extends UnaryWord {

    override def name: String = "sqrt"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Sqrt(t)

    override def summary: String =
      """
        |Compute a new time series where each interval has the square root of the value from the
        |input time series.
      """.stripMargin.trim
  }

  case object PerStep extends UnaryWord {

    override def name: String = "per-step"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.PerStep(t)

    override def summary: String =
      """
        |Converts a line from a rate per second to a rate based on the step size of the graph.
        |This is useful for getting an estimate of the raw number of events for a given
        |interval.
      """.stripMargin.trim
  }

  sealed trait BinaryWord extends SimpleWord {

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case TimeSeriesType(_) :: TimeSeriesType(_) :: _ => true
      case (_: StyleExpr) :: TimeSeriesType(_) :: _    => true
      case TimeSeriesType(_) :: (_: StyleExpr) :: _    => true
      case (_: StyleExpr) :: (_: StyleExpr) :: _       => true
    }

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case TimeSeriesType(t2) :: TimeSeriesType(t1) :: stack =>
        newInstance(t1, t2) :: stack
      case (t2: StyleExpr) :: TimeSeriesType(t1) :: stack =>
        t2.copy(expr = newInstance(t1, t2.expr)) :: stack
      case TimeSeriesType(t2) :: (t1: StyleExpr) :: stack =>
        t1.copy(expr = newInstance(t1.expr, t2)) :: stack
      case (t2: StyleExpr) :: (t1: StyleExpr) :: stack =>
        // If both sides have presentation, strip the presentation settings to avoid
        // confusion as to which settings will get used.
        newInstance(t1.expr, t2.expr) :: stack
    }

    override def signature: String = "TimeSeriesExpr TimeSeriesExpr -- TimeSeriesExpr"

    override def examples: List[String] =
      List("name,sps,:eq,42", "name,sps,:eq,:sum,name,requestsPerSecond,:eq,:max,(,name,),:by")
  }

  case object Add extends BinaryWord {

    override def name: String = "add"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.Add(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a addNaN b)` where `a`
        | and `b` are the corresponding intervals in the input time series. Sample:
        |
        || :add    | 3.0 | 0.0 | 1.0 | 1.0 | NaN |
        ||---------|-----|-----|-----|-----|-----|
        || Input 1 | 1.0 | 0.0 | 1.0 | 1.0 | NaN |
        || Input 2 | 2.0 | 0.0 | 0.0 | NaN | NaN |
        |
        |Use the [fadd](math-fadd) operator to get strict floating point behavior.
      """.stripMargin.trim
  }

  case object Subtract extends BinaryWord {

    override def name: String = "sub"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.Subtract(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a subtractNaN b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
        |
        || :sub    | 1.0 | 0.0 | 1.0 | 1.0 | NaN |
        ||---------|-----|-----|-----|-----|-----|
        || Input 1 | 2.0 | 0.0 | 1.0 | 1.0 | NaN |
        || Input 2 | 1.0 | 0.0 | 0.0 | NaN | NaN |
        |
        |Use the [fsub](math-fsub) operator to get strict floating point behavior.
      """.stripMargin.trim
  }

  case object Multiply extends BinaryWord {

    override def name: String = "mul"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.Multiply(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a * b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object Divide extends BinaryWord {

    override def name: String = "div"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.Divide(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a / b)` where `a`
        | and `b` are the corresponding intervals in the input time series. If `a` and `b` are 0,
        | then 0 will be returned for the interval. If only `b` is 0, then NaN will be returned as
        | the value for the interval. Sample data:
        |
        || :div    | 0.5 | 0.0 | NaN | NaN | NaN |
        ||---------|-----|-----|-----|-----|-----|
        || Input 1 | 1.0 | 0.0 | 1.0 | 1.0 | NaN |
        || Input 2 | 2.0 | 0.0 | 0.0 | NaN | NaN |
        |
        |Use the [fdiv](math-fdiv) operator to get strict floating point behavior.
      """.stripMargin.trim
  }

  case object Power extends BinaryWord {

    override def name: String = "pow"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.Power(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a power b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object GreaterThan extends BinaryWord {

    override def name: String = "gt"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.GreaterThan(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a > b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object GreaterThanEqual extends BinaryWord {

    override def name: String = "ge"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.GreaterThanEqual(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a >= b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object LessThan extends BinaryWord {

    override def name: String = "lt"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.LessThan(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a < b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object LessThanEqual extends BinaryWord {

    override def name: String = "le"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.LessThanEqual(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a <= b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object FAdd extends BinaryWord {

    override def name: String = "fadd"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.FAdd(t1, t2)
    }

    override def summary: String =
      """
        |Floating point addition operator. Compute a new time series where each interval has the
        | value `(a + b)` where `a` and `b` are the corresponding intervals in the input time
        | series.
        |
        || :fadd   | 3.0 | 0.0 | 1.0 | NaN | NaN |
        ||---------|-----|-----|-----|-----|-----|
        || Input 1 | 2.0 | 0.0 | 1.0 | 1.0 | NaN |
        || Input 2 | 1.0 | 0.0 | 0.0 | NaN | NaN |
        |
        |Note in many cases `NaN` will appear in data, e.g., if a node was brought up and started
        |reporting in the middle of the time window for the graph. This can lead to confusing
        |behavior if added to a line that does have data as the result will be `NaN`. Use the
        |[add](math-add) operator to treat `NaN` values as zero for combining with other time
        |series.
      """.stripMargin.trim
  }

  case object FSubtract extends BinaryWord {

    override def name: String = "fsub"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.FSubtract(t1, t2)
    }

    override def summary: String =
      """
        |Floating point subtraction operator. Compute a new time series where each interval has the
        | value `(a - b)` where `a` and `b` are the corresponding intervals in the input time
        | series.
        |
        || :fsub   | 1.0 | 0.0 | 1.0 | NaN | NaN |
        ||---------|-----|-----|-----|-----|-----|
        || Input 1 | 2.0 | 0.0 | 1.0 | 1.0 | NaN |
        || Input 2 | 1.0 | 0.0 | 0.0 | NaN | NaN |
        |
        |Note in many cases `NaN` will appear in data, e.g., if a node was brought up and started
        |reporting in the middle of the time window for the graph. This can lead to confusing
        |behavior if added to a line that does have data as the result will be `NaN`. Use the
        |[sub](math-sub) operator to treat `NaN` values as zero for combining with other time
        |series.
      """.stripMargin.trim
  }

  case object FMultiply extends BinaryWord {

    override def name: String = "fmul"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.FMultiply(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a * b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object FDivide extends BinaryWord {

    override def name: String = "fdiv"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.FDivide(t1, t2)
    }

    override def summary: String =
      """
        |Floating point division operator. Compute a new time series where each interval has the
        | value `(a / b)` where `a` and `b` are the corresponding intervals in the input time
        | series.
        |
        || :fdiv   | 2.0 | NaN | Inf | NaN | NaN |
        ||---------|-----|-----|-----|-----|-----|
        || Input 1 | 2.0 | 0.0 | 1.0 | 1.0 | NaN |
        || Input 2 | 1.0 | 0.0 | 0.0 | NaN | NaN |
        |
        |Note in many cases `NaN` will appear in data, e.g., if a node was brought up and started
        |reporting in the middle of the time window for the graph. Zero divided by zero can also
        |occur due to lack of activity in some windows. Unless you really need strict floating
        |point behavior, use the [div](math-div) operator to get behavior more appropriate for
        |graphs.
      """.stripMargin.trim
  }

  case object And extends BinaryWord {

    override def name: String = "and"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.And(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a AND b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  case object Or extends BinaryWord {

    override def name: String = "or"

    def newInstance(t1: TimeSeriesExpr, t2: TimeSeriesExpr): TimeSeriesExpr = {
      MathExpr.Or(t1, t2)
    }

    override def summary: String =
      """
        |Compute a new time series where each interval has the value `(a OR b)` where `a`
        | and `b` are the corresponding intervals in the input time series.
      """.stripMargin.trim
  }

  sealed trait AggrWord extends SimpleWord {

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: TimeSeriesExpr) :: _ => true
      case (_: StyleExpr) :: _      => true
    }

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (a: DataExpr.AggregateFunction) :: stack if this != Count =>
        // If applied to a base aggregate function, then it will be a single line
        // and be a noop unless it is a count.
        a :: stack
      case (t: TimeSeriesExpr) :: stack =>
        newInstance(t) :: stack
      case (t: StyleExpr) :: stack =>
        t.copy(expr = newInstance(t.expr)) :: stack
    }

    override def signature: String = "TimeSeriesExpr -- TimeSeriesExpr"

    override def examples: List[String] =
      List("name,sps,:eq,:sum", "name,sps,:eq,:max,(,nf.cluster,),:by")
  }

  case object Sum extends AggrWord {

    override def name: String = "sum"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Sum(t)

    override def summary: String =
      """
        |Compute the sum of all the time series that result from the previous expression.
        |To compute sum of all the time series that match a query, refer to
        |[data/:sum](data-sum) instead.
      """.stripMargin.trim
  }

  case object Count extends AggrWord {

    override def name: String = "count"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Count(t)

    override def summary: String =
      """
        |Compute the count of all the time series that result from the previous expression.
        |To compute count of all the time series that match a query, refer to
        |[data/:count](data-count) instead.
      """.stripMargin.trim
  }

  case object Min extends AggrWord {

    override def name: String = "min"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Min(t)

    override def summary: String =
      """
        |Compute the min of all the time series that result from the previous expression.
        |To compute min of all the time series that match a query, refer to
        |[data/:min](data-min) instead.
      """.stripMargin.trim
  }

  case object Max extends AggrWord {

    override def name: String = "max"

    def newInstance(t: TimeSeriesExpr): TimeSeriesExpr = MathExpr.Max(t)

    override def summary: String =
      """
        |Compute the max of all the time series that result from the previous expression.
        |To compute max of all the time series that match a query, refer to
        |[data/:max](data-max) instead.
      """.stripMargin.trim
  }

  /**
    * Compute estimated percentile values using counts for well known buckets. See spectator
    * PercentileBuckets for more information. The input will be grouped by the `percentile` key
    * with each key value being the bucket index. The output will be one line per requested
    * percentile. The value should be a sum or group by. Other aggregate types, such as max, will
    * automatically be converted to sum.
    */
  case object Percentiles extends SimpleWord {

    override def name: String = "percentiles"

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case DoubleListType(_) :: DataExprType(expr) :: _ =>
        expr match {
          case _: DataExpr.All => false
          case _               => true
        }
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case DoubleListType(pcts) :: DataExprType(t) :: stack =>
        // Percentile always needs sum aggregate type, if others are used convert to sum
        val expr = t match {
          case af: AggregateFunction => DataExpr.GroupBy(toSum(af), List(TagKey.percentile))
          case by: DataExpr.GroupBy  => DataExpr.GroupBy(toSum(by.af), TagKey.percentile :: by.keys)
          case _ =>
            throw new IllegalArgumentException(":percentiles can only be used with :sum and :by")
        }
        MathExpr.Percentiles(expr, pcts) :: stack
    }

    private def toSum(af: AggregateFunction): DataExpr.Sum = {
      DataExpr.Sum(af.query, offset = af.offset)
    }

    override def summary: String =
      """
        |Estimate percentiles for a timer or distribution summary. The data must have been
        |published appropriately to allow the approximation. If using
        |[spectator](http://netflix.github.io/spectator/en/latest/), then see
        |[PercentileTimer](http://netflix.github.io/spectator/en/latest/javadoc/spectator-api/com/netflix/spectator/api/histogram/PercentileTimer.html)
        |and
        |[PercentileDistributionSummary](http://netflix.github.io/spectator/en/latest/javadoc/spectator-api/com/netflix/spectator/api/histogram/PercentileDistributionSummary.html)
        |helper classes.
        |
        |The percentile values can be shown in the legend using `$percentile`.
        |
        |Since: 1.5.0 (first in 1.5.0-rc.4)
      """.stripMargin.trim

    override def signature: String = "DataExpr percentiles:List -- Expr"

    override def examples: List[String] = List(
      "name,requestLatency,:eq,(,25,50,90,)"
    )
  }

  /**
    * Compute the estimated number of samples within a range of the distribution for a
    * percentile approximation.
    */
  case object SampleCount extends Word {

    override def name: String = "sample-count"

    override def matches(stack: List[Any]): Boolean = {
      stack match {
        case DoubleType(_) :: DoubleType(_) :: (_: Query) :: _ => true
        case _                                                 => false
      }
    }

    private def bucketLabel(prefix: String, v: Double): String = {
      val idx = PercentileBuckets.indexOf(v.toLong)
      f"$prefix$idx%04X"
    }

    private def bucketQuery(prefix: String, min: Double, max: Double): Query = {
      val ge = Query.GreaterThanEqual(TagKey.percentile, bucketLabel(prefix, min))
      val le = Query.LessThanEqual(TagKey.percentile, bucketLabel(prefix, max))
      ge.and(le)
    }

    private def bucketQuery(min: Double, max: Double): Query = {
      // Verify values are reasonable
      require(min < max, s"min >= max (min=$min, max=$max)")
      require(min >= 0.0, s"min < 0 (min=$min)")

      // Query for the ranges of both distribution summaries and timers. This allows it
      // to work for either type.
      val distQuery = bucketQuery("D", min, max)
      val timerQuery = bucketQuery("T", min * 1e9, max * 1e9)
      distQuery.or(timerQuery)
    }

    override def execute(context: Context): Context = {
      context.stack match {
        case DoubleType(max) :: DoubleType(min) :: (q: Query) :: stack =>
          val rangeQuery = q.and(bucketQuery(min, max))
          val expr = DataExpr.Sum(rangeQuery)
          val nr = MathExpr.NamedRewrite(name, q, List(min, max), expr, context)
          context.copy(stack = nr :: stack)
        case _ =>
          invalidStack
      }
    }

    override def signature: String = "Query Double Double -- DataExpr"

    override def summary: String =
      """
        |Estimate the number of samples for a percentile approximation within a range of
        |the distribution.
        |""".stripMargin

    override def examples: List[String] = Nil
  }
}
