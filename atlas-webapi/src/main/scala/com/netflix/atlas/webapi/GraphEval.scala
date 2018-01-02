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
package com.netflix.atlas.webapi

import java.awt.Color
import java.io.ByteArrayOutputStream
import java.time.Duration

import akka.http.scaladsl.model.Uri
import com.netflix.atlas.chart.Colors
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.MessageDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.SummaryStats
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Strings

/**
  * Helper for rendering a graph image based on a query URI.
  */
object GraphEval {

  case class Result(request: GraphApi.Request, data: Array[Byte]) {

    def dataString: String = new String(data, "UTF-8")
  }

  def render(db: Database, uri: Uri): Result = {
    val request = GraphApi.toRequest(uri)
    val dataReq = request.toDbRequest
    val data = dataReq.exprs.map(expr => expr -> db.execute(dataReq.context, expr)).toMap
    render(request, data)
  }

  def render(request: GraphApi.Request, data: Map[DataExpr, List[TimeSeries]]): Result = {
    val graphDef = createGraph(request, data)
    val baos = new ByteArrayOutputStream
    request.engine.write(graphDef, baos)
    Result(request, baos.toByteArray)
  }

  def createGraph(request: GraphApi.Request, data: Map[DataExpr, List[TimeSeries]]): GraphDef = {

    val warnings = List.newBuilder[String]

    val plotExprs = request.exprs.groupBy(_.axis.getOrElse(0))
    val multiY = plotExprs.size > 1

    val palette = newPalette(request.flags.palette)
    val shiftPalette = newPalette("bw")

    val start = request.startMillis
    val end = request.endMillis

    val plots = plotExprs.toList.sortWith(_._1 < _._1).map {
      case (yaxis, exprs) =>
        val axisCfg = request.flags.axes(yaxis)
        val dfltStyle = if (axisCfg.stack) LineStyle.STACK else LineStyle.LINE

        val axisPalette = axisCfg.palette.fold(palette) { v =>
          newPalette(v)
        }

        var messages = List.empty[String]
        val lines = exprs.flatMap { s =>
          val result = s.expr.eval(request.evalContext, data)

          // Pick the last non empty message to appear. Right now they are only used
          // as a test for providing more information about the state of filtering. These
          // can quickly get complicated when used with other features. For example,
          // sorting can mix and match lines across multiple expressions. Also binary
          // math operations that combine the results of multiple filter expressions or
          // multi-level group by with filtered input. For now this is just an
          // experiment for the common simple case to see how it impacts usability
          // when dealing with filter expressions that remove some of the lines.
          if (result.messages.nonEmpty) messages = result.messages.take(1)

          val ts = result.data
          val labelledTS = ts.map { t =>
            val offset = Strings.toString(Duration.ofMillis(s.offset))
            val newT = t.withTags(t.tags + (TagKey.offset -> offset))
            newT.withLabel(s.legend(newT))
          }

          val lineDefs = labelledTS.sortWith(_.label < _.label).map { t =>
            val color = s.color.getOrElse {
              val c = if (s.offset > 0L) shiftPalette(t.label) else axisPalette(t.label)
              // Alpha setting if present will set the alpha value for the color automatically
              // assigned by the palette. If using an explicit color it will have no effect as the
              // alpha can be set directly using an ARGB hex format for the color.
              s.alpha.fold(c)(a => Colors.withAlpha(c, a))
            }

            LineDef(
              data = t,
              color = color,
              lineStyle = s.lineStyle.fold(dfltStyle)(s => LineStyle.valueOf(s.toUpperCase)),
              lineWidth = s.lineWidth,
              legendStats = SummaryStats(t.data, start, end)
            )
          }

          // Lines must be sorted for presentation after the colors have been applied
          // using the palette. The colors selected should be stable regardless of the
          // sort order that is applied. Otherwise colors would change each time a user
          // changed the sort.
          val sorted = sort(warnings, s.sortBy, s.useDescending, lineDefs)
          s.limit.fold(sorted)(n => sorted.take(n))
        }

        // Apply sort based on URL parameters. This will take precedence over
        // local sort on an expression.
        val sortedLines = sort(warnings, axisCfg.sort, axisCfg.order.contains("desc"), lines)

        axisCfg.newPlotDef(sortedLines ::: messages.map(s => MessageDef(s"... $s ...")), multiY)
    }

    request.newGraphDef(plots, warnings.result())
  }

  /**
    * Creates a new palette and optionally changes it to use the label hash for
    * selecting the color rather than choosing the next available color in the
    * palette. Hash selection is useful to ensure that the same color is always
    * used for a given label even on separate graphs. However, it also means
    * that collisions are more likely and that the same color may be used for
    * different labels even with a small number of lines.
    *
    * Hash mode will be used if the palette name is prefixed with "hash:".
    */
  private def newPalette(mode: String): String => Color = {
    val prefix = "hash:"
    if (mode.startsWith(prefix)) {
      val pname = mode.substring(prefix.length)
      val p = Palette.create(pname)
      v =>
        p.colors(v.hashCode)
    } else {
      val p = Palette.create(mode).iterator
      _ =>
        p.next()
    }
  }

  private def sort(
    warnings: scala.collection.mutable.Builder[String, List[String]],
    sortBy: Option[String],
    useDescending: Boolean,
    lines: List[LineDef]
  ): List[LineDef] = {

    // The default is sort by legend in ascending order. If the defaults have been explicitly
    // changed, then the explicit values should be used. Since the sort by param is used to
    // short circuit if there is nothing to do, it will get set to legend explicitly here if
    // the order has been changed to descending.
    val by = if (useDescending) Some(sortBy.getOrElse("legend")) else sortBy

    by.fold(lines) { mode =>
      val cmp: Function2[LineDef, LineDef, Boolean] = mode match {
        case "legend" =>
          (a, b) =>
            compare(useDescending, a.data.label, b.data.label)
        case "min" =>
          (a, b) =>
            compare(useDescending, a.legendStats.min, b.legendStats.min)
        case "max" =>
          (a, b) =>
            compare(useDescending, a.legendStats.max, b.legendStats.max)
        case "avg" =>
          (a, b) =>
            compare(useDescending, a.legendStats.avg, b.legendStats.avg)
        case "count" =>
          (a, b) =>
            compare(useDescending, a.legendStats.count, b.legendStats.count)
        case "total" =>
          (a, b) =>
            compare(useDescending, a.legendStats.total, b.legendStats.total)
        case "last" =>
          (a, b) =>
            compare(useDescending, a.legendStats.last, b.legendStats.last)
        case order =>
          warnings += s"Invalid sort mode '$order'. Using default of 'legend'."
          (a, b) =>
            compare(useDescending, a.data.label, b.data.label)
      }
      lines.sortWith(cmp)
    }
  }

  private def compare(desc: Boolean, a: String, b: String): Boolean = {
    if (desc) a > b else a < b
  }

  private def compare(desc: Boolean, a: Int, b: Int): Boolean = {
    if (desc) a > b else a < b
  }

  private def compare(desc: Boolean, a: Double, b: Double): Boolean = {

    // Note: NaN values are special and should always be sorted last. This is the default
    // behavior of `JDouble.compare` for strictly greater than or less than. However it does
    // mean that you cannot change the order by sorting one way and then reversing because that
    // would move the NaN values to the beginning.
    // https://github.com/Netflix/atlas/issues/586
    if (desc) compare(_ > _, a, b) else compare(_ < _, a, b)
  }

  private def compare(op: (Double, Double) => Boolean, a: Double, b: Double): Boolean = {

    // Do not use op directly because NaN values can cause contract errors with the sort:
    // https://github.com/Netflix/atlas/issues/405
    if (a.isNaN && b.isNaN)
      false
    else if (a.isNaN)
      false // b should come first as it has a value
    else if (b.isNaN)
      true // a should come first as it has a value
    else
      op(a, b)
  }
}
