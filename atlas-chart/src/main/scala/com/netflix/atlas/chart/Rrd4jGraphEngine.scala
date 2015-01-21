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
package com.netflix.atlas.chart

import java.awt.BasicStroke
import java.awt.Color
import java.awt.Paint
import java.awt.Stroke
import java.awt.image.RenderedImage
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.time.Duration
import java.time.ZonedDateTime
import java.util.Calendar
import java.util.Date
import java.util.Locale
import java.util.TimeZone

import com.netflix.atlas.core.model.SummaryStats
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Math
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.core.util.UnitPrefix
import org.rrd4j.data.Plottable
import org.rrd4j.graph.RrdAxisDef
import org.rrd4j.graph.RrdGraph
import org.rrd4j.graph.RrdGraphConstants
import org.rrd4j.graph.RrdGraphDef
import org.rrd4j.graph.TimeLabelFormat


object Rrd4jGraphEngine {
  private class CustomTimeLabelFormat extends TimeLabelFormat {
    def format(calendar: Calendar, locale: Locale, date: Date): String = {
      val c = calendar.clone().asInstanceOf[Calendar]
      c.setTime(date)
      if (c.get(Calendar.MILLISECOND) != 0)
        String.format(locale, ".%1$tL", c)
      else if (c.get(Calendar.SECOND) != 0)
        String.format(locale, ":%1$tS", c)
      else if (c.get(Calendar.MINUTE) != 0)
        String.format(locale, "%1$tH:%1$tM", c)
      else if (c.get(Calendar.HOUR_OF_DAY) != 0)
        String.format(locale, "%1$tH:%1$tM", c)
      else if (c.get(Calendar.DAY_OF_MONTH) != 1)
        String.format(locale, "%1$tb%1$td", c)
      else if (c.get(Calendar.DAY_OF_YEAR) != 1)
        String.format(locale, "%1$tb%1$td", c)
      else
        String.format(locale, "%1$tY", c)
    }
  }
}

class Rrd4jGraphEngine extends PngGraphEngine {

  import com.netflix.atlas.chart.LineStyle._

  type PaletteMap = collection.mutable.Map[String, Palette]

  def name: String = "png"

  private def dashedStroke: Stroke = {
    new BasicStroke(
      1.0f,
      BasicStroke.CAP_BUTT,
      BasicStroke.JOIN_MITER,
      1.0f,
      Array(1.0f, 1.0f),
      0.0f)
  }

  private def addStats(config: GraphDef, graphDef: RrdGraphDef, s: SeriesDef) {

    if (s.style != LineStyle.VSPAN) {
      if (config.showLegendStats) {
        val stats = SummaryStats(s.data, config.startTime.toEpochMilli, config.endTime.toEpochMilli)

        graphDef.comment("%s%s%s%s\\l".format(
          "    Max :",   UnitPrefix.format(stats.max,   "%9.3f%1s"),
          "     Min  :", UnitPrefix.format(stats.min,   "%9.3f%1s")))
        graphDef.comment("%s%s%s%s\\l".format(
          "    Avg :",   UnitPrefix.format(stats.avg,   "%9.3f%1s"),
          "     Last :", UnitPrefix.format(stats.last,  "%9.3f%1s")))
        graphDef.comment("%s%s%s%s\\l".format(
          "    Tot :",   UnitPrefix.format(stats.total, "%9.3f%1s"),
          "     Cnt  :", UnitPrefix.format(stats.count, "%9.3f%1s")))
        graphDef.comment("\\l")
      }
    }
  }

  private def newPaletteMap: collection.mutable.Map[String, Palette] = {
    collection.mutable.Map.empty[String, Palette]
  }

  private def applyAlpha(alpha: Option[Int], color: Color): Color = {
    alpha.fold(color)(a => new Color(color.getRed, color.getGreen, color.getBlue, a))
  }

  private def isNearlyZero(v: Double): Boolean = {
    scala.math.abs(v - 0.0) < 1e-12
  }

  private def addSpans(
      config: GraphDef,
      graphDef: RrdGraphDef,
      id: String,
      color: Paint,
      legend: String,
      data: TimeSeries) {
    val step = config.step
    val end = config.endTime.toEpochMilli
    var pos = config.startTime.toEpochMilli
    var vstart = -1L
    var legendVar = legend
    while (pos <= end) {
      if (isNearlyZero(data.data(pos))) {
        if (vstart != -1) {
          graphDef.vspan(vstart / 1000, pos / 1000, color, legendVar)
          if (legendVar != null) {
            legendVar = null
            graphDef.comment("\\l")
          }
          vstart = -1
        }
      } else {
        if (vstart == -1) vstart = pos
      }
      pos += step
    }
    if (vstart != -1) {
      graphDef.vspan(vstart / 1000, end / 1000, color, legendVar)
      if (legendVar != null)
        graphDef.comment("\\l")
    }
  }

  private def setMessage(graphDef: RrdGraphDef, msg: String) {
    graphDef.setShowSignature(true)
    graphDef.setSignature(msg)
  }

  override def write(config: GraphDef, output: OutputStream) {
    val graphDef = new RrdGraphDef

    graphDef.setShowSignature(false)
    graphDef.setTimeLabelFormat(new Rrd4jGraphEngine.CustomTimeLabelFormat)
    graphDef.setColor(RrdGraphConstants.COLOR_XAXIS, new Color(0, 0, 0, 50))
    graphDef.setColor(RrdGraphConstants.COLOR_YAXIS, new Color(0, 0, 0, 50))

    val smallFont = RrdGraphConstants.DEFAULT_SMALL_FONT
    val largeFont = RrdGraphConstants.DEFAULT_LARGE_FONT
    val titleFont = smallFont.deriveFont(largeFont.getSize.toFloat)
    graphDef.setFont(RrdGraphConstants.FontTag.TITLE, titleFont)
    config.fontSize.foreach { fontSize =>
      graphDef.setFont(RrdGraphConstants.FontTag.DEFAULT, smallFont.deriveFont(fontSize.toFloat))
    }

    config.title.foreach(graphDef.setTitle)
    graphDef.setImageFormat("png")
    graphDef.setGridStroke(dashedStroke)
    graphDef.setTextAntiAliasing(true)
    graphDef.setAntiAliasing(false)
    graphDef.setRigid(true)
    graphDef.setOnlyGraph(config.onlyGraph)

    if (!config.showBorder) {
      graphDef.setColor(RrdGraphConstants.COLOR_SHADEA, RrdGraphConstants.DEFAULT_BACK_COLOR)
      graphDef.setColor(RrdGraphConstants.COLOR_SHADEB, RrdGraphConstants.DEFAULT_BACK_COLOR)
    }

    graphDef.setTimeZone(TimeZone.getTimeZone(config.timezone))

    val start = config.startTime.toEpochMilli / 1000
    val end = config.endTime.toEpochMilli / 1000
    graphDef.setTimeSpan(start, end)
    graphDef.setStep(config.step / 1000)

    if (config.height <= GraphConstants.MaxHeight) {
      graphDef.setHeight(config.height)
    } else {
      graphDef.setHeight(GraphConstants.MaxHeight)
      setMessage(graphDef, s"restricted graph height to ${GraphConstants.MaxHeight}")
    }

    if (config.width <= GraphConstants.MaxWidth) {
      graphDef.setWidth(config.width)
    } else {
      graphDef.setWidth(GraphConstants.MaxWidth)
      setMessage(graphDef, s"restricted graph width to ${GraphConstants.MaxWidth}")
    }

    val graphLines = config.plots.map(_.series.size).sum
    val axisPerLine = config.axisPerLine && graphLines <= (GraphConstants.MaxYAxis + 1)
    if (axisPerLine != config.axisPerLine) {
      setMessage(graphDef, s"axis per line disabled, $graphLines lines")
    }

    graphDef.setNoLegend(!config.showLegend || graphLines > GraphConstants.MaxLinesInLegend)
    if (config.showLegend && graphLines > GraphConstants.MaxLinesInLegend) {
      setMessage(graphDef, s"legend suppressed, $graphLines lines")
    }

    config.verticalSpans.foreach { span =>
      val t1 = span.t1.toEpochMilli / 1000
      val t2 = span.t2.toEpochMilli / 1000
      graphDef.vspan(t1, t2, span.color, null)
    }

    val axes = collection.mutable.Map.empty[Int, AxisDef]
    (0 to GraphConstants.MaxYAxis).foreach { k =>
      if ((axisPerLine && k < graphLines) || config.axis.contains(k)) {
        val axisConfig = config.axis.getOrElse(k, new AxisDef(k != 0))
        axes += k -> axisConfig

        val rrdAxisDef = new RrdAxisDef()
        rrdAxisDef.setOpposite(axisConfig.rightSide)

        axisConfig.min.foreach(m => rrdAxisDef.setMinValue(m))
        axisConfig.max.foreach(m => rrdAxisDef.setMaxValue(m))
        axisConfig.label.foreach(rrdAxisDef.setVerticalLabel)

        rrdAxisDef.setLogarithmic(axisConfig.logarithmic)

        graphDef.addValueAxis(k, rrdAxisDef)
      }
    }

    val palettes = newPaletteMap
    var nextID = -1
    val firstStack = new Array[Boolean](graphDef.getAxisCount)

    config.plots.foreach { plotConfig =>

      plotConfig.horizontalSpans.foreach { span =>
        graphDef.hspan(span.axis, span.v1, span.v2, span.color, null)
      }

      plotConfig.series.foreach { series =>
        nextID += 1
        val id = nextID.toString
        val yaxis = if (axisPerLine) nextID else series.axis.getOrElse(0)
        val axisDef = axes(yaxis)
        val label = series.label

        val color = resolveColors(palettes, graphDef, series, yaxis)
        val vcolor = config.visionType.convert(color)

        val legend = label + "\\l"
        graphDef.datasource(id, new SeriesPlottable(series))

        if (axisDef.stack) {
          if (series.style != VSPAN) {
            series.style = STACK
          }
        }

        series.style match {
          case LINE =>
            val lw = new BasicStroke(series.lineWidth)
            graphDef.line(yaxis, id, vcolor, legend, lw, false)
          case AREA =>
            graphDef.area(yaxis, id, vcolor, legend, false)
          case STACK =>
            if (!firstStack(yaxis)) {
              graphDef.area(yaxis, id, vcolor, legend, false)
              firstStack(yaxis) = true
            } else {
              graphDef.stack(yaxis, id, vcolor, legend)
            }
          case VSPAN =>
            addSpans(config, graphDef, id, vcolor, legend, series.data)
        }

        addStats(config, graphDef, series)
      }
    }

    val frame = Strings.toString(Duration.between(config.startTime, config.endTime))
    val endTime = ZonedDateTime.ofInstant(config.endTime, config.timezone).toString
    val step = Strings.toString(Duration.ofMillis(config.step))
    val comment = "Frame: %s, End: %s, Step: %s\\l".format(frame, endTime, step)
    graphDef.comment(comment)

    if (config.loadTime > 0 && config.stats.inputLines > 0) {
      val graphLines = config.plots.map(_.series.size).sum
      val graphDatapoints = graphLines * ((end - start) / (config.step / 1000) + 1)
      val stats = "Fetch: %sms (L: %s, %s, %s; D: %s, %s, %s)\\l".format(
        config.loadTime.toString,
        UnitPrefix.format(config.stats.inputLines),
        UnitPrefix.format(config.stats.outputLines),
        UnitPrefix.format(graphLines),
        UnitPrefix.format(config.stats.inputDatapoints),
        UnitPrefix.format(config.stats.outputDatapoints),
        UnitPrefix.format(graphDatapoints)
      )
      graphDef.comment(stats)
    } else if (config.loadTime > 0) {
      val stats = "Fetch: %sms\\l".format(config.loadTime.toString)
      graphDef.comment(stats)
    }

    config.notices.foreach { n =>
      graphDef.comment("\\l")
      n match {
        case Info(msg)    => graphDef.comment("INFO: " + msg + "\\j")
        case Warning(msg) => graphDef.comment("WARNING: " + msg + "\\j")
        case Error(msg)   => graphDef.comment("ERROR: " + msg + "\\j")
      }
    }

    val graph = new RrdGraph(graphDef)
    output.write(graph.getRrdGraphInfo.getBytes)
  }

  def resolveColors(pp: PaletteMap, graphDef: RrdGraphDef, s: SeriesDef, axis: Int): Color  = {
    if (graphDef.getAxisCount == 1)  {
      s.color.getOrElse(applyAlpha(s.alpha, pickColor(pp,s)))
    } else {
      val axisDef =  graphDef.getAxisDef(axis)
      if (s.color.isEmpty) {
          val axisColor = setAxisColorFromSeriesPallette(axisDef, pp, s)
          applyAlpha(s.alpha, axisColor)
      }  else {
          setAxisColorFromSeriesColor(axisDef, pp, s)
          s.color.get
      }
    }
  }

  def pickColor(pp: PaletteMap, s: SeriesDef) : Color = {
    val p = s.palette
    val color = pp.getOrElseUpdate(p, Palette(p)).nextColor
    color
  }

  def setAxisColorFromSeriesPallette(axisDef: RrdAxisDef, pp: PaletteMap, s: SeriesDef): Color = {
     if (axisDef.getColor == null) {
        axisDef.setColor(pickColor(pp, s))
     }
    axisDef.getColor.asInstanceOf[Color]
  }

  def setAxisColorFromSeriesColor(axisDef: RrdAxisDef, pp: PaletteMap, s: SeriesDef): Color = {
    if (axisDef.getColor == null) {
      val c = s.color.get
      axisDef.setColor(new Color(c.getRed, c.getGreen, c.getBlue))
    }
    axisDef.getColor.asInstanceOf[Color]
  }

  def createImage(config: GraphDef): RenderedImage = {
    val buf = new ByteArrayOutputStream
    write(config, buf)
    PngImage(buf.toByteArray).data
  }

  private class SeriesPlottable(s: SeriesDef) extends Plottable {

    val ts = if (s.style == STACK) s.data.mapTimeSeq(_.mapValues(v => Math.addNaN(v, 0.0))) else s.data

    override def getValue(timestamp: Long): Double = {
      ts.data(timestamp * 1000)
    }
  }
}
