/*
 * Copyright 2014-2017 Netflix, Inc.
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
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.MessageDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotBound
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.chart.model.TickLabelMode
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

    val warnings = List.newBuilder[String]

    val plotExprs = request.exprs.groupBy(_.axis.getOrElse(0))
    val multiY = plotExprs.size > 1

    val palette = newPalette(request.flags.palette)
    val shiftPalette = newPalette("bw")

    val start = request.startMillis
    val end = request.endMillis

    val plots = plotExprs.toList.sortWith(_._1 < _._1).map { case (yaxis, exprs) =>
      val axisCfg = request.flags.axes(yaxis)
      val dfltStyle = if (axisCfg.stack) LineStyle.STACK else LineStyle.LINE

      val axisPalette = axisCfg.palette.fold(palette) { v => newPalette(v) }

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
            legendStats = SummaryStats(t.data, start, end))
        }

        // Lines must be sorted for presentation after the colors have been applied
        // using the palette. The colors selected should be stable regardless of the
        // sort order that is applied. Otherwise colors would change each time a user
        // changed the sort.
        sort(warnings, s.sortBy, s.useDescending, lineDefs)
      }

      // Apply sort based on URL parameters. This will take precedence over
      // local sort on an expression.
      val sortedLines = sort(warnings, axisCfg.sort, axisCfg.order.contains("desc"), lines)

      PlotDef(
        data = sortedLines ::: messages.map(s => MessageDef(s"... $s ...")),
        lower = axisCfg.lower.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
        upper = axisCfg.upper.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
        ylabel = axisCfg.ylabel,
        scale = Scale.fromName(axisCfg.scale.getOrElse("linear")),
        axisColor = if (multiY) None else Some(Color.BLACK),
        tickLabelMode = axisCfg.tickLabels.fold(TickLabelMode.DECIMAL)(TickLabelMode.apply))
    }

    val graphDef = request.newGraphDef(plots, warnings.result())

    val baos = new ByteArrayOutputStream
    request.engine.write(graphDef, baos)

    Result(request, baos.toByteArray)
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
      v => p.colors(v.hashCode)
    } else {
      val p = Palette.create(mode).iterator
      _ => p.next()
    }
  }

  private def sort(
    warnings: scala.collection.mutable.Builder[String, List[String]],
    sortBy: Option[String],
    useDescending: Boolean,
    lines: List[LineDef]): List[LineDef] = {

    import java.lang.{Double => JDouble}
    sortBy.fold(lines) { mode =>
      val cmp: Function2[LineDef, LineDef, Boolean] = mode match {
        case "legend" => (a, b) => a.data.label < b.data.label
        case "min"    => (a, b) => JDouble.compare(a.legendStats.min, b.legendStats.min) < 0
        case "max"    => (a, b) => JDouble.compare(a.legendStats.max, b.legendStats.max) < 0
        case "avg"    => (a, b) => JDouble.compare(a.legendStats.avg, b.legendStats.avg) < 0
        case "count"  => (a, b) => a.legendStats.count < b.legendStats.count
        case "total"  => (a, b) => JDouble.compare(a.legendStats.total, b.legendStats.total) < 0
        case "last"   => (a, b) => JDouble.compare(a.legendStats.last, b.legendStats.last) < 0
        case order    =>
          warnings += s"Invalid sort mode '$order'. Using default of 'legend'."
          (a, b) => a.data.label < b.data.label
      }
      val sorted = lines.sortWith(cmp)
      if (useDescending) sorted.reverse else sorted
    }
  }
}
