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
package com.netflix.atlas.chart

import java.awt.Color
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant
import java.time.ZoneId
import java.util.Base64
import scala.util.Using
import com.netflix.atlas.chart.graphics.ChartSettings
import com.netflix.atlas.chart.model.*
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.CollectorStats
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.core.util.Strings
import tools.jackson.core.JsonGenerator
import tools.jackson.core.JsonParser
import tools.jackson.core.ObjectReadContext
import tools.jackson.core.ObjectWriteContext
import tools.jackson.core.json.JsonFactory
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper

/**
  * Helper for converting a graph definition to and from json. The format is still being tested
  * and is not yet considered final. To allow data to be incrementally written in the future, this
  * format uses an array with entries following the pattern:
  *
  * ```
  * [graph-metadata,... plot-metadata ..., ... lines ..., ...]
  * ```
  *
  * Metadata is output first so something could setup initial rendering. Then we output the
  * `plot-metadata` that corresponds to all lines on a given axis. The plot has an id that
  * will be referenced when the line data is emitted.
  */
object JsonCodec {

  import com.netflix.atlas.json.JsonParserHelper.*
  private val factory = new JsonFactory()

  private val mapper = JsonMapper
    .builder(factory)
    .disable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    .build()

  private val pngEngine = new DefaultGraphEngine

  def encode(config: GraphDef): String = {
    Streams.string { w =>
      Using.resource(factory.createGenerator(ObjectWriteContext.empty, w)) { gen =>
        writeGraphDef(gen, config)
      }
    }
  }

  def encode(output: OutputStream, config: GraphDef): Unit = {
    Using.resource(factory.createGenerator(ObjectWriteContext.empty, output)) { gen =>
      writeGraphDef(gen, config)
    }
  }

  def decode(json: String): GraphDef = {
    Using.resource(factory.createParser(ObjectReadContext.empty, json)) { parser =>
      readGraphDef(parser)
    }
  }

  def decode(json: InputStream): GraphDef = {
    Using.resource(factory.createParser(ObjectReadContext.empty, json)) { parser =>
      readGraphDef(parser)
    }
  }

  private def writeGraphDef(gen: JsonGenerator, config: GraphDef): Unit = {
    gen.writeStartArray()
    writeGraphImage(gen, config)
    writeGraphDefMetadata(gen, config)
    config.plots.zipWithIndex.foreach {
      case (plot, i) =>
        writePlotDefMetadata(gen, plot, i)
        writeHeatmapDef(gen, config, plot, i)
    }
    config.plots.zipWithIndex.foreach {
      case (plot, i) =>
        plot.data.foreach { data =>
          writeDataDef(gen, i, data, config.startTime.toEpochMilli, config.endTime.toEpochMilli)
        }
    }
    gen.writeEndArray()
  }

  // Writes out a pre-rendered image for the chart. This can be used
  // for partially dynamic views.
  private def writeGraphImage(gen: JsonGenerator, config: GraphDef): Unit = {
    if (!config.renderingHints.contains("no-image")) {
      gen.writeStartObject()
      gen.writeStringProperty("type", "graph-image")
      gen.writeStringProperty("data", toDataUri(config))
      gen.writeEndObject()
    }
  }

  private def toDataUri(config: GraphDef): String = {
    val image = PngImage(pngEngine.createImage(config)).toByteArray
    val encoded = Base64.getEncoder.encodeToString(image)
    s"data:image/png;base64,$encoded"
  }

  def writeGraphDefMetadata(gen: JsonGenerator, config: GraphDef): Unit = {
    gen.writeStartObject()
    gen.writeStringProperty("type", "graph-metadata")
    gen.writeNumberProperty("startTime", config.startTime.toEpochMilli)
    gen.writeNumberProperty("endTime", config.endTime.toEpochMilli)
    gen.writeArrayPropertyStart("timezones")
    config.timezones.foreach { tz =>
      gen.writeString(tz.getId)
    }
    gen.writeEndArray()
    gen.writeNumberProperty("step", config.step)

    gen.writeNumberProperty("width", config.width)
    gen.writeNumberProperty("height", config.height)
    gen.writeStringProperty("layout", config.layout.name())
    gen.writeNumberProperty("zoom", config.zoom)

    config.title.foreach { t =>
      gen.writeStringProperty("title", t)
    }
    gen.writeStringProperty("legendType", config.legendType.name())
    gen.writeBooleanProperty("onlyGraph", config.onlyGraph)
    gen.writeStringProperty("theme", config.themeName)

    if (config.loadTime > 0) {
      gen.writeNumberProperty("loadTime", config.loadTime)
    }

    if (config.stats != CollectorStats.unknown) {
      gen.writeObjectPropertyStart("stats")
      gen.writeNumberProperty("inputLines", config.stats.inputLines)
      gen.writeNumberProperty("inputDatapoints", config.stats.inputDatapoints)
      gen.writeNumberProperty("outputLines", config.stats.outputLines)
      gen.writeNumberProperty("outputDatapoints", config.stats.outputDatapoints)
      gen.writeEndObject()
    }

    gen.writeArrayPropertyStart("warnings")
    config.warnings.foreach(gen.writeString)
    gen.writeEndArray()

    if (config.renderingHints.nonEmpty) {
      gen.writeArrayPropertyStart("renderingHints")
      config.renderingHints.foreach(gen.writeString)
      gen.writeEndArray()
    }

    gen.writeEndObject()
  }

  def writePlotDefMetadata(gen: JsonGenerator, plot: PlotDef, id: Int): Unit = {
    gen.writeStartObject()
    gen.writeStringProperty("type", "plot-metadata")
    gen.writeNumberProperty("id", id)
    plot.ylabel.foreach { v =>
      gen.writeStringProperty("ylabel", v)
    }
    plot.axisColor.foreach { c =>
      gen.writeName("axisColor")
      writeColor(gen, c)
    }
    gen.writeStringProperty("scale", plot.scale.name())
    gen.writeStringProperty("upper", plot.upper.toString)
    gen.writeStringProperty("lower", plot.lower.toString)
    gen.writeStringProperty("tickLabelMode", plot.tickLabelMode.name())
    gen.writeEndObject()
  }

  private def writeHeatmapDef(gen: JsonGenerator, graph: GraphDef, plot: PlotDef, id: Int): Unit = {
    plot.heatmapData(graph).foreach { heatmap =>
      gen.writeStartObject()
      gen.writeStringProperty("type", "heatmap")
      gen.writeNumberProperty("plot", id)
      gen.writeStringProperty("colorScale", heatmap.settings.colorScale.name())
      gen.writeStringProperty("upper", heatmap.settings.upper.toString)
      gen.writeStringProperty("lower", heatmap.settings.lower.toString)
      heatmap.settings.label.foreach { label =>
        gen.writeStringProperty("label", label)
      }

      // Y-tick information, used to define the vertical buckets for heatmap counts. Included
      // so the result can be reproduced in a dynamic rendering.
      gen.writeArrayPropertyStart("yTicks")
      var min = heatmap.yaxis.min
      var i = 0
      while (i < heatmap.yTicks.size) {
        val max = heatmap.yTicks(i).v
        gen.writeStartObject()
        gen.writeNumberProperty("min", min)
        gen.writeNumberProperty("max", max)
        gen.writeStringProperty("label", heatmap.yTicks(i).label)
        gen.writeEndObject()
        min = max
        i += 1
      }
      gen.writeEndArray()

      // Color ticks used to map counts to a color
      gen.writeArrayPropertyStart("colorTicks")
      val colorTicks = heatmap.colorTicks
      min = heatmap.minCount
      i = 1
      while (i < colorTicks.size) {
        val max = colorTicks(i).v
        gen.writeStartObject()
        gen.writeName("color")
        writeColor(gen, heatmap.palette.colors(i - 1))
        gen.writeNumberProperty("min", min)
        gen.writeNumberProperty("max", max)
        gen.writeStringProperty("label", colorTicks(i).label)
        gen.writeEndObject()
        min = max
        i += 1
      }
      gen.writeEndArray()

      // Output the counts associated with each cell
      gen.writeObjectPropertyStart("data")
      gen.writeStringProperty("type", "heatmap")
      gen.writeArrayPropertyStart("values")
      var t = heatmap.xaxis.start
      while (t < heatmap.xaxis.end) {
        gen.writeStartArray()
        var y = 0
        while (y < heatmap.numberOfValueBuckets) {
          gen.writeNumber(heatmap.count(t, y))
          y += 1
        }
        gen.writeEndArray()
        t += graph.step
      }
      gen.writeEndArray()
      gen.writeEndObject()

      gen.writeEndObject()
    }
  }

  private def writeDataDef(
    gen: JsonGenerator,
    plot: Int,
    data: DataDef,
    start: Long,
    end: Long
  ): Unit = {
    data match {
      case v: LineDef    => writeLineDef(gen, plot, v, start, end)
      case v: HSpanDef   => writeHSpanDef(gen, plot, v)
      case v: VSpanDef   => writeVSpanDef(gen, plot, v)
      case v: MessageDef => writeMessageDef(gen, plot, v)
    }
  }

  private def writeLineDef(
    gen: JsonGenerator,
    plot: Int,
    line: LineDef,
    start: Long,
    end: Long
  ): Unit = {
    gen.writeStartObject()
    gen.writeStringProperty("type", "timeseries")
    line.query.foreach { q =>
      val id = TaggedItem.computeId(line.data.tags + ("atlas.query" -> q)).toString
      gen.writeStringProperty("id", id)
    }
    gen.writeNumberProperty("plot", plot)
    gen.writeStringProperty("label", line.data.label)
    gen.writeName("color")
    writeColor(gen, line.color)
    gen.writeStringProperty("lineStyle", line.lineStyle.name())
    gen.writeNumberProperty("lineWidth", line.lineWidth)
    line.query.foreach { q =>
      gen.writeStringProperty("query", q)
    }
    if (line.groupByKeys.nonEmpty) {
      gen.writeArrayPropertyStart("groupByKeys")
      line.groupByKeys.foreach(gen.writeString)
      gen.writeEndArray()
    }
    gen.writeObjectPropertyStart("tags")
    line.data.tags.foreachEntry(gen.writeStringProperty)
    gen.writeEndObject()
    gen.writeObjectPropertyStart("data")
    gen.writeStringProperty("type", "array")
    gen.writeArrayPropertyStart("values")
    line.data.data.foreach(start, end) { (_, v) =>
      gen.writeNumber(v)
    }
    gen.writeEndArray()
    gen.writeEndObject()
    gen.writeEndObject()
  }

  private def writeHSpanDef(gen: JsonGenerator, plot: Int, span: HSpanDef): Unit = {
    gen.writeStartObject()
    gen.writeStringProperty("type", "hspan")
    gen.writeNumberProperty("plot", plot)
    span.labelOpt.foreach { v =>
      gen.writeStringProperty("label", v)
    }
    gen.writeName("color")
    writeColor(gen, span.color)
    gen.writeNumberProperty("v1", span.v1)
    gen.writeNumberProperty("v2", span.v2)
    gen.writeEndObject()
  }

  private def writeVSpanDef(gen: JsonGenerator, plot: Int, span: VSpanDef): Unit = {
    gen.writeStartObject()
    gen.writeStringProperty("type", "vspan")
    gen.writeNumberProperty("plot", plot)
    span.labelOpt.foreach { v =>
      gen.writeStringProperty("label", v)
    }
    gen.writeName("color")
    writeColor(gen, span.color)
    gen.writeNumberProperty("t1", span.t1.toEpochMilli)
    gen.writeNumberProperty("t2", span.t2.toEpochMilli)
    gen.writeEndObject()
  }

  private def writeMessageDef(gen: JsonGenerator, plot: Int, msg: MessageDef): Unit = {
    gen.writeStartObject()
    gen.writeStringProperty("type", "message")
    gen.writeNumberProperty("plot", plot)
    gen.writeStringProperty("label", msg.label)
    gen.writeName("color")
    writeColor(gen, msg.color)
    gen.writeEndObject()
  }

  private def writeColor(gen: JsonGenerator, color: Color): Unit = {
    gen.writeString(Strings.zeroPad(color.getRGB, 8))
  }

  private def readGraphDef(parser: JsonParser): GraphDef = {
    var gdef: GraphDef = null
    val plots = Map.newBuilder[Int, PlotDef]
    val heatmaps = Map.newBuilder[Int, HeatmapDef]
    val data = List.newBuilder[(Int, DataDef)]
    foreachItem(parser) {
      val node = mapper.readTree[JsonNode](parser)
      node.get("type").asString() match {
        case "graph-image" =>
        // ignored for right now
        case "graph-metadata" =>
          if (gdef != null)
            throw new IllegalStateException("multiple graph-metadata blocks")
          gdef = toGraphDef(node)
        case "plot-metadata" =>
          plots += node.get("id").asInt(0) -> toPlotDef(node)
        case "heatmap" =>
          val plot = node.get("plot").asInt(0)
          heatmaps += plot -> toHeatmapDef(node)
        case "timeseries" =>
          val plot = node.get("plot").asInt(0)
          data += plot -> toLineDef(gdef, node)
        case "hspan" =>
          val plot = node.get("plot").asInt(0)
          data += plot -> toHSpanDef(node)
        case "vspan" =>
          val plot = node.get("plot").asInt(0)
          data += plot -> toVSpanDef(node)
        case "message" =>
          val plot = node.get("plot").asInt(0)
          data += plot -> toMessageDef(node)
      }
    }

    val heatmapData = heatmaps.result()
    val groupedData = data.result().groupBy(_._1)

    val sortedPlots = plots.result().toList.sortWith(_._1 < _._1)
    val plotList = sortedPlots.map {
      case (id, plot) =>
        val plotLines = groupedData.get(id).map(_.map(_._2)).getOrElse(Nil)
        plot.copy(data = plotLines, heatmap = heatmapData.get(id))
    }

    gdef.copy(plots = plotList)
  }

  private def toGraphDef(node: JsonNode): GraphDef = {

    // format: off
    import scala.jdk.CollectionConverters.*
    GraphDef(
      Nil,
      startTime      = Instant.ofEpochMilli(node.get("startTime").asLong()),
      endTime        = Instant.ofEpochMilli(node.get("endTime").asLong()),
      timezones      = node.get("timezones").iterator().asScala.map(n => ZoneId.of(n.asString())).toList,
      step           = node.get("step").asLong(),
      width          = node.get("width").asInt(),
      height         = node.get("height").asInt(),
      layout         = Layout.valueOf(node.get("layout").asString()),
      zoom           = node.get("zoom").asDouble(),
      title          = Option(node.get("title")).map(_.asString()),
      legendType     = LegendType.valueOf(node.get("legendType").asString()),
      onlyGraph      = node.get("onlyGraph").asBoolean(),
      loadTime       = Option(node.get("loadTime")).fold(-1L)(_.asLong()),
      stats          = Option(node.get("stats")).fold(CollectorStats.unknown)(toCollectorStats),
      warnings       = node.get("warnings").iterator().asScala.map(_.asString()).toList,
      themeName      = Option(node.get("theme")).fold(ChartSettings.defaultTheme)(_.asString()),
      renderingHints = processRenderingHints(node.get("renderingHints"))
    )
    // format: on
  }

  private def toCollectorStats(node: JsonNode): CollectorStats = {

    // format: off
    CollectorStats(
      inputLines       = node.get("inputLines").asLong(),
      inputDatapoints  = node.get("inputDatapoints").asLong(),
      outputLines      = node.get("outputLines").asLong(),
      outputDatapoints = node.get("outputDatapoints").asLong()
    )
    // format: on
  }

  private def processRenderingHints(node: JsonNode): Set[String] = {
    import scala.jdk.CollectionConverters.*
    if (node == null)
      Set.empty
    else
      node.iterator().asScala.map(_.asString()).toSet
  }

  private def toPlotDef(node: JsonNode): PlotDef = {

    // format: off
    PlotDef(
      Nil,
      ylabel        = Option(node.get("ylabel")).map(_.asString()),
      axisColor     = Option(node.get("axisColor")).map(toColor),
      scale         = Scale.valueOf(node.get("scale").asString()),
      upper         = PlotBound(node.get("upper").asString()),
      lower         = PlotBound(node.get("lower").asString()),
      tickLabelMode = TickLabelMode.valueOf(node.get("tickLabelMode").asString())
    )
    // format: on
  }

  /**
    * Need to make sure alpha is handled properly, it will get ignored in some cases with the
    * color class.
    *
    * ```
    * scala> val c = new Color(Integer.parseUnsignedInt("32FF0000", 16))
    * c: java.awt.Color = java.awt.Color[r=255,g=0,b=0]
    *
    * scala> c.getAlpha
    * res0: Int = 255
    * ```
    */
  private def toColor(node: JsonNode): Color = Strings.parseColor(node.asString())

  private def toStringList(node: JsonNode): List[String] = {
    if (node != null && node.isArray) {
      val builder = List.newBuilder[String]
      val iter = node.iterator()
      while (iter.hasNext) {
        builder += iter.next().asString()
      }
      builder.result()
    } else {
      Nil
    }
  }

  private def toHeatmapDef(node: JsonNode): HeatmapDef = {
    import scala.jdk.CollectionConverters.*
    val colors = node
      .get("colorTicks")
      .iterator()
      .asScala
      .map { node =>
        toColor(node.get("color"))
      }
      .toArray
    HeatmapDef(
      colorScale = Scale.valueOf(node.get("colorScale").asString()),
      upper = PlotBound(node.get("upper").asString()),
      lower = PlotBound(node.get("lower").asString()),
      palette = Some(Palette.fromArray("heatmap", colors)),
      label = Option(node.get("label")).map(_.asString())
    )
  }

  private def toLineDef(gdef: GraphDef, node: JsonNode): LineDef = {
    LineDef(
      data = toTimeSeries(gdef, node),
      query = Option(node.get("query")).map(_.asString()),
      groupByKeys = toStringList(node.get("groupByKeys")),
      color = toColor(node.get("color")),
      lineStyle = LineStyle.valueOf(node.get("lineStyle").asString()),
      lineWidth = node.get("lineWidth").asDouble().toFloat
    )
  }

  private def toHSpanDef(node: JsonNode): HSpanDef = {
    HSpanDef(
      v1 = node.get("v1").asDouble(),
      v2 = node.get("v2").asDouble(),
      color = toColor(node.get("color")),
      labelOpt = Option(node.get("label")).map(_.asString())
    )
  }

  private def toVSpanDef(node: JsonNode): VSpanDef = {
    VSpanDef(
      t1 = Instant.ofEpochMilli(node.get("t1").asLong()),
      t2 = Instant.ofEpochMilli(node.get("t2").asLong()),
      color = toColor(node.get("color")),
      labelOpt = Option(node.get("label")).map(_.asString())
    )
  }

  private def toMessageDef(node: JsonNode): MessageDef = {
    MessageDef(
      color = toColor(node.get("color")),
      label = node.get("label").asString()
    )
  }

  private def toTimeSeries(gdef: GraphDef, node: JsonNode): TimeSeries = {
    import scala.jdk.CollectionConverters.*
    val tags = node.get("tags").properties.asScala.map(e => e.getKey -> e.getValue.asString()).toMap
    val values = node.get("data").get("values").iterator().asScala.map(toDouble).toArray
    val seq = new ArrayTimeSeq(DsType.Gauge, gdef.startTime.toEpochMilli, gdef.step, values)
    TimeSeries(tags, node.get("label").asString(), seq)
  }

  private def toDouble(node: JsonNode): Double = {
    if (node.isNumber)
      node.asDouble()
    else if (node.isString)
      node.stringValue().toDouble
    else
      Double.NaN
  }
}
