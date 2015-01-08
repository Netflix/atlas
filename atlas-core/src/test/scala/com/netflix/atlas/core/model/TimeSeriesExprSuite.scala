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

import java.time.temporal.ChronoUnit

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.util.Math
import org.scalatest.FunSuite

import scala.language.postfixOps

class TimeSeriesExprSuite extends FunSuite {

  import com.netflix.atlas.core.model.TimeSeriesExprSuite._

  val interpreter = Interpreter(StyleVocabulary.words ::: StandardVocabulary.words)
  val data = constants

  val noTags = Map.empty[String, String]
  val constTag = "type" -> "constant"

  val tests = List(
    ":true,:all"                  -> const(constants),
    ":true"                       -> const(ts(constTag, 55)),
    ":true,:sum"                  -> const(ts(constTag, 55)),
    ":true,:count"                -> const(ts(constTag, 11)),
    ":true,:avg"                  -> const(ts(constTag, "(type=constant / type=constant)", 5)),
    ":true,:min"                  -> const(ts(constTag, 0)),
    ":true,:max"                  -> const(ts(constTag, 10)),
    "name,:has"                   -> const(ts(constTag, 55)),
    "name,1,:eq"                  -> const(ts(Map("name" -> "1", constTag), 1)),
    "name,1,:re"                  -> const(ts(constTag, 11)),
    "name,2,:re"                  -> const(ts(Map("name" -> "2", constTag), 2)),
    "name,(,1,10,),:in"           -> const(ts(constTag, 11)),
    "name,1,:eq,name,10,:eq,:or"  -> const(ts(constTag, 11)),
    ":true,:abs"                  -> const(ts(constTag, "abs(type=constant)", 55.0)),
    "10,:abs"                     -> const(ts(Map("name" -> "10.0"), "abs(10.0)", 10.0)),
    "-10,:abs"                    -> const(ts(Map("name" -> "-10.0"), "abs(-10.0)", 10.0)),
    ":true,:neg"                  -> const(ts(constTag, "neg(type=constant)", -55.0)),
    "10,:neg"                     -> const(ts(Map("name" -> "10.0"), "neg(10.0)", -10.0)),
    "-10,:neg"                    -> const(ts(Map("name" -> "-10.0"), "neg(-10.0)", 10.0)),
    "10,:neg,:abs"                -> const(ts(Map("name" -> "10.0"), "abs(neg(10.0))", 10.0)),
    "4,:sqrt"                     -> const(ts(Map("name" -> "4.0"), "sqrt(4.0)", 2.0)),
    ":true,10,:add"               -> const(ts(constTag, "(type=constant + 10.0)", 55.0 + 10)),
    ":true,10,:sub"               -> const(ts(constTag, "(type=constant - 10.0)", 55.0 - 10)),
    ":true,10,:mul"               -> const(ts(constTag, "(type=constant * 10.0)", 55.0 * 10)),
    ":true,10,:div"               -> const(ts(constTag, "(type=constant / 10.0)", 55.0 / 10)),
    ":true,0,:div"                -> const(ts(constTag, "(type=constant / 0.0)", Double.NaN)),
    ":true,55,:gt"                -> const(ts(constTag, "(type=constant > 55.0)", 0.0)),
    ":true,0,:gt"                 -> const(ts(constTag, "(type=constant > 0.0)", 1.0)),
    ":true,55.1,:ge"              -> const(ts(constTag, "(type=constant >= 55.1)", 0.0)),
    ":true,55,:ge"                -> const(ts(constTag, "(type=constant >= 55.0)", 1.0)),
    ":true,0,:ge"                 -> const(ts(constTag, "(type=constant >= 0.0)", 1.0)),
    ":true,55,:lt"                -> const(ts(constTag, "(type=constant < 55.0)", 0.0)),
    ":true,56,:lt"                -> const(ts(constTag, "(type=constant < 56.0)", 1.0)),
    ":true,55.1,:le"              -> const(ts(constTag, "(type=constant <= 55.1)", 1.0)),
    ":true,55,:le"                -> const(ts(constTag, "(type=constant <= 55.0)", 1.0)),
    ":true,0,:le"                 -> const(ts(constTag, "(type=constant <= 0.0)", 0.0)),
    ":true,0,:and"                -> const(ts(constTag, "(type=constant AND 0.0)", 0.0)),
    ":true,1,:and"                -> const(ts(constTag, "(type=constant AND 1.0)", 1.0)),
    ":true,0,:or"                 -> const(ts(constTag, "(type=constant OR 0.0)", 1.0)),
    ":true,1,:or"                 -> const(ts(constTag, "(type=constant OR 1.0)", 1.0)),
    "0,0,:or"                     -> const(ts("name" -> "0.0", "(0.0 OR 0.0)", 0.0)),
    "1,:per-step"                 -> const(ts(Map("name" -> "1.0"), "per-step(1.0)", 60)),
    "1,:integral"                 -> const(integral(Map("name" -> "1.0"), "integral(1.0)", 1.0)),
    "minuteOfDay,:time,1,:add"    -> const(integral(Map("name" -> "minuteOfDay"), "(minuteOfDay + 1.0)", 1.0)),
    "1,:integral,:derivative"     -> const(ts(Map("name" -> "1.0"), "derivative(integral(1.0))", 1.0)),
    "8,:integral"                 -> const(integral(Map("name" -> "8.0"), "integral(8.0)", 8.0)),
    ":true,:integral"             -> const(integral(Map(constTag), "integral(type=constant)", 55.0)),
    //"1,PT5M,:trend"               -> const(ts(Map("name" -> "1.0"), "trend(1.0, PT5M)", 1.0)),
    //"NaN,PT5M,:trend"             -> const(ts(Map("name" -> "NaN"), "trend(NaN, PT5M)", Double.NaN)),
    //":random"                     -> const(ts(Map("name" -> "random"), "random", Double.NaN)),
    //":random,PT5M,:trend"         -> const(ts(Map("name" -> "random"), "trend(random, PT5M)", Double.NaN)),
    //"1,:integral,PT2M,:trend"     -> const(ts(Map("name" -> "random"), "trend(integral(random), PT2M)", Double.NaN)),
    //"1,5,0.1,0.5,:des"            -> const(ts(Map("name" -> "1.0"), "des(1.0)", 1.0)),
    //":false"                -> const(Nil)
    "1,10,:rolling-count"         -> const(integral(Map("name" -> "1.0"), "rolling-count(1.0, 10)", 1.0)),
    "8,10,:rolling-count"         -> const(integral(Map("name" -> "8.0"), "rolling-count(8.0, 10)", 1.0)),
    "0,10,:rolling-count"         -> const(integral(Map("name" -> "0.0"), "rolling-count(0.0, 10)", 0.0)),
    "1,2,:rolling-count"          -> const(rollingCount2),
    ":false,:all"                 -> const(Nil),
    ":true,(,name,),:by"          -> const(byName),
    ":true,(,name,),:by,1,:add"   -> const(byNamePlus1),
    "1,:true,(,name,),:by,:add"   -> const(byNamePlus1Lhs),
    ":true,(,name,),:by,:dup,:add"-> const(byNamePlusByName),
    ":true,(,type,),:by"          -> const(ts(constTag, "(type=constant)", 55)),
    ":true,(,name,),:by,:sum"     -> const(ts(constTag, "type=constant", 55)),
    ":true,:sum,(,name,),:by,:sum"-> const(ts(constTag, "type=constant", 55)),
    ":true,:min,(,name,),:by,:min"-> const(ts(constTag, "type=constant", 0)),
    ":true,:max,(,name,),:by,:max"-> const(ts(constTag, "type=constant", 10)),
    ":true,:max,(,type,),:by,:sum"-> const(ts(constTag, "sum(type=constant)", 10)),
    ":true,(,foo,),:by"           -> const(Nil),
    "NaN,NaN,:add"                -> const(ts(Map("name" -> "NaN"), "(NaN + NaN)", Double.NaN)),
    "NaN,1.0,:add"                -> const(ts(Map("name" -> "NaN"), "(NaN + 1.0)", 1.0)),
    "1.0,NaN,:add"                -> const(ts(Map("name" -> "1.0"), "(1.0 + NaN)", 1.0)),
    "2.0,1.0,:add"                -> const(ts(Map("name" -> "2.0"), "(2.0 + 1.0)", 3.0)),
    "NaN,NaN,:sub"                -> const(ts(Map("name" -> "NaN"), "(NaN - NaN)", Double.NaN)),
    "NaN,1.0,:sub"                -> const(ts(Map("name" -> "NaN"), "(NaN - 1.0)", -1.0)),
    "1.0,NaN,:sub"                -> const(ts(Map("name" -> "1.0"), "(1.0 - NaN)", 1.0)),
    "2.0,1.0,:sub"                -> const(ts(Map("name" -> "2.0"), "(2.0 - 1.0)", 1.0)),
    "NaN,NaN,:mul"                -> const(ts(Map("name" -> "NaN"), "(NaN * NaN)", Double.NaN)),
    "NaN,1.0,:mul"                -> const(ts(Map("name" -> "NaN"), "(NaN * 1.0)", Double.NaN)),
    "1.0,NaN,:mul"                -> const(ts(Map("name" -> "1.0"), "(1.0 * NaN)", Double.NaN)),
    "2.0,1.0,:mul"                -> const(ts(Map("name" -> "2.0"), "(2.0 * 1.0)", 2.0)),
    "NaN,NaN,:div"                -> const(ts(Map("name" -> "NaN"), "(NaN / NaN)", Double.NaN)),
    "NaN,1.0,:div"                -> const(ts(Map("name" -> "NaN"), "(NaN / 1.0)", Double.NaN)),
    "1.0,NaN,:div"                -> const(ts(Map("name" -> "1.0"), "(1.0 / NaN)", Double.NaN)),
    "2.0,1.0,:div"                -> const(ts(Map("name" -> "2.0"), "(2.0 / 1.0)", 2.0)),
    "NaN,NaN,:fadd"               -> const(ts(Map("name" -> "NaN"), "(NaN + NaN)", Double.NaN)),
    "NaN,1.0,:fadd"               -> const(ts(Map("name" -> "NaN"), "(NaN + 1.0)", Double.NaN)),
    "1.0,NaN,:fadd"               -> const(ts(Map("name" -> "1.0"), "(1.0 + NaN)", Double.NaN)),
    "2.0,1.0,:fadd"               -> const(ts(Map("name" -> "2.0"), "(2.0 + 1.0)", 3.0)),
    "NaN,NaN,:fsub"               -> const(ts(Map("name" -> "NaN"), "(NaN - NaN)", Double.NaN)),
    "NaN,1.0,:fsub"               -> const(ts(Map("name" -> "NaN"), "(NaN - 1.0)", Double.NaN)),
    "1.0,NaN,:fsub"               -> const(ts(Map("name" -> "1.0"), "(1.0 - NaN)", Double.NaN)),
    "2.0,1.0,:fsub"               -> const(ts(Map("name" -> "2.0"), "(2.0 - 1.0)", 1.0)),
    "NaN,NaN,:fmul"               -> const(ts(Map("name" -> "NaN"), "(NaN * NaN)", Double.NaN)),
    "NaN,1.0,:fmul"               -> const(ts(Map("name" -> "NaN"), "(NaN * 1.0)", Double.NaN)),
    "1.0,NaN,:fmul"               -> const(ts(Map("name" -> "1.0"), "(1.0 * NaN)", Double.NaN)),
    "2.0,1.0,:fmul"               -> const(ts(Map("name" -> "2.0"), "(2.0 * 1.0)", 2.0)),
    "NaN,NaN,:fdiv"               -> const(ts(Map("name" -> "NaN"), "(NaN / NaN)", Double.NaN)),
    "NaN,1.0,:fdiv"               -> const(ts(Map("name" -> "NaN"), "(NaN / 1.0)", Double.NaN)),
    "1.0,NaN,:fdiv"               -> const(ts(Map("name" -> "1.0"), "(1.0 / NaN)", Double.NaN)),
    "2.0,1.0,:fdiv"               -> const(ts(Map("name" -> "2.0"), "(2.0 / 1.0)", 2.0)),
    "42"                          -> const(ts(42))
  )

  tests.map { case (prg, p) =>
    test(s"eval global: $prg") {
      val c = interpreter.execute(prg)
      assert(c.stack.size === 1)
      val expr = c.stack.collect { case Extractors.TimeSeriesType(t) => t } head
      val rs = expr.eval(p.ctxt, p.input)
      assert(rs.expr === expr)
      assert(bounded(rs.data, p.ctxt) === bounded(p.output, p.ctxt))
    }

    List("1m" -> 60000, "5m" -> 300000).foreach { case (label, step) =>
      test(s"eval incremental $label: $prg") {
        val c = interpreter.execute(prg)
        assert(c.stack.size === 1)
        val expr = c.stack.collect { case Extractors.TimeSeriesType(t) => t} head
        val rs = expr.eval(p.ctxt, p.input)
        val ctxts = p.ctxt.partition(step, ChronoUnit.MINUTES)
        var state = Map.empty[StatefulExpr, Any]
        val incrResults = ctxts.map { ctxt =>
          val c = ctxt.copy(state = state)
          val results = expr.eval(c, bounded(p.input, ctxt))
          state = results.state
          val boundedResults = results.data.map(_.copy(_.bounded(ctxt.start, ctxt.end)))
          boundedResults.groupBy(_.id)
        }
        val incrRS = ResultSet(expr, rs.data.map { t =>
          val seq = incrResults.head(t.id).head.data.bounded(p.ctxt.start, p.ctxt.end)
          incrResults.tail.foreach { m =>
            seq.update(m(t.id).head.data)(Math.maxNaN)
          }
          TimeSeries(t.tags, t.label, seq)
        })
        assert(incrRS.expr === expr)
        assert(bounded(incrRS.data, p.ctxt) === bounded(p.output, p.ctxt))
      }
    }
  }

  def rollingCount2: TimeSeries = {
    val seq = new FunctionTimeSeq(DsType.Gauge, 60000, t => if (t == 0L) 1.0 else 2.0)
    TimeSeries(Map("name" -> "1.0"), "rolling-count(1.0, 2)", seq)
  }

  def byName: List[TimeSeries] = constants.zipWithIndex.map { case (t, i) =>
    TimeSeries(t.tags, s"(name=${i.toString})", t.data)
  }

  def byNamePlus1: List[TimeSeries] = constants.zipWithIndex.map { case (t, i) =>
    TimeSeries(t.tags, s"((name=${i.toString}) + 1.0)", t.data.mapValues(_ + 1.0))
  }

  def byNamePlus1Lhs: List[TimeSeries] = constants.zipWithIndex.map { case (t, i) =>
    TimeSeries(t.tags, s"(1.0 + (name=${i.toString}))", t.data.mapValues(_ + 1.0))
  }

  def byNamePlusByName: List[TimeSeries] = constants.zipWithIndex.map { case (t, i) =>
    TimeSeries(t.tags, s"((name=${i.toString}) + (name=${i.toString}))", t.data.mapValues(_ * 2.0))
  }

  def integral(tags: Map[String, String], label: String, amount: Double): TimeSeries = {
    // Assumes starting at time 0, for amount calculation
    val seq = new FunctionTimeSeq(DsType.Gauge, 60000, t => amount + amount * (t / 60000))
    TimeSeries(tags, label, seq)
  }

  def const(t: TimeSeries): Params = Params(constants, List(t))
  def const(ts: List[TimeSeries]): Params = Params(constants, ts)

  def ts(t: (String, String), v: Double): TimeSeries = ts(Map.empty + t, v)

  def ts(t: (String, String), label: String, v: Double): TimeSeries = ts(Map.empty + t, label, v)

  def ts(tags: Map[String, String], v: Double): TimeSeries = {
    val seq = new FunctionTimeSeq(DsType.Gauge, 60000, _ => v)
    TimeSeries(tags, seq)
  }

  def ts(tags: Map[String, String], label: String, v: Double): TimeSeries = {
    val seq = new FunctionTimeSeq(DsType.Gauge, 60000, _ => v)
    TimeSeries(tags, label, seq)
  }

  def ts(v: Double): TimeSeries = {
    val seq = new FunctionTimeSeq(DsType.Gauge, 60000, _ => v)
    TimeSeries(Map("name" -> v.toString), v.toString, seq)
  }

  def bounded(data: List[TimeSeries], ctxt: EvalContext): List[TimeSeries] = {
    data.sortWith(_.label < _.label).map { ts => ts.copy(_.bounded(ctxt.start, ctxt.end)) }
  }

  def constants: List[TimeSeries] = {
    val ts = (0 to 10).map { i =>
      val seq = new FunctionTimeSeq(DsType.Gauge, 60000, _ => i)
      TimeSeries(Map("name" -> i.toString, "type" -> "constant"), seq)
    }
    ts.toList
  }
}

object TimeSeriesExprSuite {
  case class Params(
      input: List[TimeSeries],
      output: List[TimeSeries],
      ctxt: EvalContext = EvalContext(0, 10 * 60000, 60000))
}
