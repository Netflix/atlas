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

import java.awt.Color
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.Json
import com.netflix.atlas.test.GraphAssertions
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite


abstract class PngGraphEngineSuite extends FunSuite with BeforeAndAfterAll {

  private val dataDir   = s"graphengine/data"

  // SBT working directory gets updated with fork to be the dir for the project
  private val baseDir = "."
  private val goldenDir = s"$baseDir/src/test/resources/graphengine/${getClass.getSimpleName}"
  private val targetDir = s"$baseDir/target/${getClass.getSimpleName}"
  private val graphAssertions = new GraphAssertions(goldenDir, targetDir)

  val bless = false

  def prefix: String
  def graphEngine: PngGraphEngine

  val step = 60000

  def constant(v: Double): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
  }

  def wave(min: Double, max: Double, wavelength: Duration): TimeSeries = {
    val lambda = 2 * scala.math.Pi / wavelength.toMillis
    def f(t: Long): Double = {
      val amp = (max - min) / 2.0
      val yoffset = min + amp
      amp * scala.math.sin(t * lambda) + yoffset
    }
    TimeSeries(Map("name" -> "wave"), new FunctionTimeSeq(DsType.Gauge, step, f))
  }

  def interval(ts1: TimeSeries, ts2: TimeSeries, s: Long, e: Long): TimeSeries = {
    def f(t: Long): Double = {
      val ts = if (t >= s && t < e) ts2 else ts1
      ts.data(t)
    }
    TimeSeries(Map("name" -> "interval"), new FunctionTimeSeq(DsType.Gauge, step, f))
  }

  def finegrainWave(min: Int, max: Int, hours: Int): TimeSeries = {
    wave(min, max, Duration.ofHours(hours))
  }

  def finegrainSeriesDef(min: Int, max: Int, hours: Int): SeriesDef = {
    val seriesDef = new SeriesDef
    seriesDef.data = finegrainWave(min ,max, hours)
    seriesDef
  }

  def simpleWave(min: Int, max: Int): TimeSeries = {
    wave(min, max, Duration.ofDays(1))
  }

  def simpleWave(max: Int): TimeSeries = {
      simpleWave(0, max)
  }

  def simpleSeriesDef(min: Int, max: Int): SeriesDef = {
    val seriesDef = new SeriesDef
    seriesDef.data = simpleWave(min ,max)
    seriesDef
  }

  def simpleSeriesDef(max: Int) : SeriesDef = {
    simpleSeriesDef(0, max)
  }

  def outageSeriesDef(max: Int): SeriesDef = {
    val start1 = ZonedDateTime.of(2012, 1, 1, 5, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end1 = ZonedDateTime.of(2012, 1, 1, 6, 38, 0, 0, ZoneOffset.UTC).toInstant

    val start2 = ZonedDateTime.of(2012, 1, 1, 7, 4, 0, 0, ZoneOffset.UTC).toInstant
    val end2 = ZonedDateTime.of(2012, 1, 1, 7, 5, 0, 0, ZoneOffset.UTC).toInstant

    val bad = constant(0)
    val normal = interval(simpleWave(max), bad, start1.toEpochMilli, end1.toEpochMilli)
    val seriesDef = new SeriesDef
    seriesDef.data = interval(normal, bad, start2.toEpochMilli, end2.toEpochMilli)
    seriesDef
  }

  def constantSeriesDef(value: Double) : SeriesDef = {
    val seriesDef = new SeriesDef
    seriesDef.data = constant(value)
    seriesDef
  }

  def makeTranslucent(c: Color): Color = {
     new Color(c.getRed, c.getGreen, c.getBlue, 75)
  }

  override def afterAll() {
    graphAssertions.generateReport(getClass)
  }

  def label(vs: SeriesDef*): List[SeriesDef] = {
    vs.zipWithIndex.foreach { case (v, i) => v.label = i.toString }
    vs.toList
  }

  def load(resource: String): GraphDef = {
    Streams.scope(Streams.resource(resource)) { in => Json.decode[GraphData](in).toGraphDef }
  }

  /*
  Query to find single wide spikes from garbage collection metrics:
  http://atlas-main.us-east-1.test.netflix.net:7001/api/v1/graph?q=nf.cluster,
  atlas_backend-publish,:eq,class,GarbageCollectorMXBean,:eq,:and,nf.node,i-274a814a,
  :eq,:and,name,collectionTime,:eq,:and,id,PS_MarkSweep,:eq,:and,:sum&s=e-1d
  &format=json
   */
  test("non_uniformly_drawn_spikes") {

    val name         = prefix + "_non_uniformly_drawn_spikes.png"
    val dataFileName = prefix + "_non_uniformly_drawn_spikes.json"

    val graphDef = load(s"$dataDir/$dataFileName")
    //atlas generated sample is 780 wide less 64 origin less 16 r side padding == 700
    //expect to see width of spikes vary as x values repeat due to rounding
    //RrdGraph calculates x values based on number of pixels/second
    graphDef.width = 700
    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("one_data_point_wide_spike") {

    val name = prefix + "_one_data_point_wide_spike.png"
    val tags = Map("name" -> "beehive_honeycomb.counter.full.index.success")

    val graphDef = new GraphDef
    graphDef.width = 1100
    graphDef.height = 200
    graphDef.startTime = ZonedDateTime.of(2013, 6,  9, 18, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2013, 6, 11, 18, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.step = 3*60000  // one sample every 3 minute

    // 2 days of samples with 3 minute intervals inclusive start-end
    val sampleCnt =  1 +  (2 * 1440) / 3
    val values = new Array[Double](sampleCnt)

    values(0) =  0.005553

    //examples of spike values skipped with step 288 calculated by RrdGraph
    //based on pixel count of 600
    values(10) =  0.005553

    //roughly at the location of the spike reported missing in jira CLDMTA-1449
    values(690) = 0.005553

    //last data point spike drawn high to end of graph axis,
    //(e.g. one pixel wide trailing bar)
    values(sampleCnt-1)  =  0.005553

    val seq = new ArrayTimeSeq(DsType.Gauge,
      graphDef.startTime.toEpochMilli, graphDef.step, values)
    val seriesDef = new SeriesDef
    seriesDef.data = TimeSeries(tags, seq)

    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)
    graphDef.axis(0).max = Some(0.005553 * 1.5)
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line") {
    val name = prefix + "_single_line.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_with_load_time") {
    val name = prefix + "_single_line_with_load_time.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime   = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)
    graphDef.loadTime = 5123L

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_only_graph") {
    val name = prefix + "_single_line_only_graph.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.onlyGraph = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_no_border") {
    val name = prefix + "_single_line_no_border.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.showBorder = false
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_title") {
    val name = prefix + "_single_line_title.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.title = Some("A sample title")
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_timezone") {
    val name = prefix + "_single_line_timezone.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.timezone = ZoneId.of("US/Pacific")
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_no_legend") {
    val name = prefix + "_single_line_no_legend.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.showLegend = false
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_no_legend_stats") {
    val name = prefix + "_single_line_no_legend_stats.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.showLegendStats = false
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_linewidth") {
    val name = prefix + "_single_line_linewidth.png"

    val seriesDef = simpleSeriesDef(400)
    seriesDef.lineWidth = 3.0f
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_logarithmic") {
    val name = prefix + "_single_line_logarithmic.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.axis(0).logarithmic = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_upper") {
    val name = prefix + "_single_line_upper.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.axis(0).max = Some(200)
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_lower") {
    val name = prefix + "_single_line_lower.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.axis(0).min = Some(200)
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)
    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_ylabel") {
    val name = prefix + "_single_line_ylabel.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.axis(0).label = Some("something useful")
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_area") {
    val name = prefix + "_single_line_area.png"

    val seriesDef = simpleSeriesDef(400)
    seriesDef.style = LineStyle.AREA

    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_color") {
    val name = prefix + "_single_line_color.png"

    val seriesDef = simpleSeriesDef(400)
    seriesDef.color = Some(Color.BLUE)

    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_stack") {
    val name = prefix + "_single_line_stack.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.axis.get(0).get.stack = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_stack_negative") {
    val name = prefix + "_single_line_stack_negative.png"

    val seriesDef = simpleSeriesDef(-400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.axis.get(0).get.stack = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_hspans") {
    val name = prefix + "_single_line_hspans.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)
    plotDef.horizontalSpans = List(
      HSpan(0, 300, 400, Color.RED, None),
      HSpan(0, 5, 42, Color.BLUE, Some("really bad error")))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("single_line_vspans") {
    val name = prefix + "_single_line_vspans.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

    val errStart = ZonedDateTime.of(2012, 1, 1, 4, 0, 0, 0, ZoneOffset.UTC).toInstant
    val errEnd = ZonedDateTime.of(2012, 1, 1, 8, 30, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.verticalSpans = List(VSpan(errStart, errEnd, Color.RED, None))
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("double_line") {
    val name = prefix + "_double_line.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef, simpleSeriesDef(150))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("axis_per_line") {
    val name = prefix + "_axis_per_line.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef, simpleSeriesDef(150))

    val graphDef = new GraphDef
    graphDef.axisPerLine = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("double_line_stack") {
    val name = prefix + "_double_line_stack.png"

    val seriesDef = simpleSeriesDef(400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef, simpleSeriesDef(150))

    val graphDef = new GraphDef
    graphDef.axis.get(0).get.stack = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("double_line_stack_on_NaN") {
    val name = prefix + "_double_line_stack_on_NaN.png"

    val plotDef = new PlotDef
    plotDef.series = label(constantSeriesDef(Double.NaN), simpleSeriesDef(150))

    val graphDef = new GraphDef
    graphDef.axis.get(0).get.stack = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("double_line_stack_middle_NaN") {
    val name = prefix + "_double_line_stack_middle_NaN.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(150),constantSeriesDef(Double.NaN), simpleSeriesDef(300))

    val graphDef = new GraphDef
    graphDef.axis.get(0).get.stack = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("double_line_stack_negative") {
    val name = prefix + "_double_line_stack_negative.png"

    val seriesDef = simpleSeriesDef(-400)
    val plotDef = new PlotDef
    plotDef.series = label(seriesDef, simpleSeriesDef(150))

    val graphDef = new GraphDef
    graphDef.axis.get(0).get.stack = true
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("double_yaxis") {
    val name = prefix + "_double_yaxis.png"

    val plotDef1 = new PlotDef
    plotDef1.series = label(simpleSeriesDef(40000), simpleSeriesDef(42))

    val plotDef2 = new PlotDef
    plotDef2.series = label(simpleSeriesDef(400), simpleSeriesDef(150))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef1, plotDef2)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("vspans_from_line") {
    val name = prefix + "_vspans_from_line.png"

    val dataDef = outageSeriesDef(400)
    dataDef.lineWidth = 2.0f

    val spanDef = outageSeriesDef(400)
    spanDef.style = LineStyle.VSPAN
    spanDef.alpha = Some(40)

    val plotDef = new PlotDef
    plotDef.series = label(dataDef, spanDef)

    val graphDef = new GraphDef
    graphDef.width = 1200
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 4, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 1, 8, 0, 0, 0, ZoneOffset.UTC).toInstant

    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  /*
   * Multi - Y Axis Test Suite
   * Default axis 0 on the left and axis 1 and 2 on the right
   *
   * Graph Element       Default Palette
   * Axis 0              RED
   *   Line A
   *   Line B
   *
   * Axis 1              GREEN
   *   Line C
   *
   * Axis 2              BLUE
   *   Line D
   *   Line E
   */
  private val LINE_A_COLOR     = new Color(247,109, 17) //orange
  private val LINE_B_COLOR     = new Color(188, 96,235) //purple
  private val LINE_C_COLOR     = new Color(139,181, 47) //yellow green
  private val LINE_D_COLOR     = new Color(123,132,138) //gray
  private val LINE_E_COLOR     = new Color(84 ,154,204) //medium blue

  def multiYDataSet(title: String, positive: Boolean = true): GraphDef = {
    val graphDef = new GraphDef
    graphDef.title = Some(title)

    val axis1Def = new AxisDef
    axis1Def.rightSide = true

    val axis2Def = new AxisDef
    axis2Def.rightSide = true

    graphDef.axis += (1 -> axis1Def, 2 -> axis2Def)

    val seriesDefA = if (positive) finegrainSeriesDef(200000, 400000, 12)
                     else  finegrainSeriesDef(-200000, -400000, 12)

    val seriesDefB = if (positive) finegrainSeriesDef(100000, 900000, 6)
                     else finegrainSeriesDef(-100000, -900000, 6)

    val seriesDefC = if (positive) constantSeriesDef(150)
                     else constantSeriesDef(150)

    val seriesDefD = if (positive) simpleSeriesDef(20000000)
                     else simpleSeriesDef(-20000000)

    val seriesDefE = if (positive) simpleSeriesDef(10000000, 30000000)
                     else simpleSeriesDef(-10000000, -30000000)

    val plotDef = new PlotDef
    plotDef.series = label(seriesDefA, seriesDefB, seriesDefC, seriesDefD, seriesDefE)

    plotDef.series(2).axis = Some(1)
    plotDef.series(3).axis = Some(2)
    plotDef.series(4).axis = Some(2)

    graphDef.plots = List(plotDef)
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

    graphDef
  }

  test("MultiY_axis_no_lines") {
    val name = prefix + "_MultiY_axis_no_lines.png"

    val graphDef = new GraphDef
    graphDef.title = Some("Axis without data plotted")

    val axis1Def = new AxisDef
    val axis2Def = new AxisDef

    val axis3Def = new AxisDef
    axis3Def.rightSide = true

    val axis4Def = new AxisDef
    axis4Def.rightSide = true

    val axis5Def = new AxisDef
    axis5Def.rightSide = true

    val axis6Def = new AxisDef
    axis6Def.rightSide = true

    val axis7Def = new AxisDef
    axis7Def.rightSide = true

    graphDef.axis += (1 -> axis1Def, 2 -> axis2Def, 3 -> axis3Def, 4 -> axis4Def, 5 -> axis5Def, 6 -> axis6Def, 7 -> axis7Def)

    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_only_graph") {
    val name = prefix + "_MultiY_only_graph.png"

    val graphDef = multiYDataSet("Data plotted for axis 0, 1, 2")
    graphDef.onlyGraph = true

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_vertical_Label") {
    val name = prefix + "_MultiY_vertical_Label.png"

    val graphDef = multiYDataSet("Vertical Labels for each axis drawn in same color as axis")

    graphDef.axis.get(0).get.label  = Some("Default y axis  - 0")
    graphDef.axis.get(1).get.label  = Some("y axis - 1")
    graphDef.axis.get(2).get.label  = Some("y axis - 2")

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_color_default") {
    val name = prefix + "_MultiY_color_default.png"

    val graphDef = multiYDataSet("Atlas Chooses axis color from default palette, line color pinned to axis color")

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_color_override_line") {
    val name = prefix + "_MultiY_color_override_line.png"

    val graphDef = multiYDataSet("Line Colors specified & Opaque Axis Color chosen from 1st line on the Axis")

    graphDef.plots(0).series(0).color = Some(makeTranslucent(LINE_A_COLOR))
    graphDef.plots(0).series(1).color = Some(LINE_B_COLOR)
    graphDef.plots(0).series(2).color = Some(LINE_C_COLOR)
    graphDef.plots(0).series(3).color = Some(LINE_D_COLOR)
    graphDef.plots(0).series(4).color = Some(LINE_E_COLOR)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_area") {
    val name = prefix + "_MultiY_area.png"

    val graphDef = multiYDataSet("Opaque Area 0 & 1 on Axis 0, Translucent Area 3 & 4 on Axis 2")

    graphDef.plots(0).series(0).style = LineStyle.AREA
    graphDef.plots(0).series(1).style = LineStyle.AREA
    graphDef.plots(0).series(3).style = LineStyle.AREA
    graphDef.plots(0).series(4).style = LineStyle.AREA

    graphDef.plots(0).series(0).color = Some(LINE_A_COLOR)
    graphDef.plots(0).series(1).color = Some(LINE_B_COLOR)
    graphDef.plots(0).series(2).color = Some(LINE_C_COLOR)
    graphDef.plots(0).series(3).color = Some(makeTranslucent(LINE_D_COLOR))
    graphDef.plots(0).series(4).color = Some(makeTranslucent(LINE_E_COLOR))

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_stack") {
    val name = prefix + "_MultiY_stack.png"

    val graphDef = multiYDataSet("Opaque stack on axis 0, Translucent stack on axis 2")

    graphDef.axis(0).stack = true
    graphDef.axis(2).stack = true

    graphDef.plots(0).series(0).color = Some(LINE_A_COLOR)
    graphDef.plots(0).series(1).color = Some(LINE_B_COLOR)
    graphDef.plots(0).series(2).color = Some(LINE_C_COLOR)
    graphDef.plots(0).series(3).color = Some(makeTranslucent(LINE_D_COLOR))
    graphDef.plots(0).series(4).color = Some(makeTranslucent(LINE_E_COLOR))

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_stack_negative") {
    val name = prefix + "_MultiY_stack_negative.png"

    val graphDef = multiYDataSet("Negative Stack: Opaque stack on axis 0, Translucent stack on axis 2", false)

    graphDef.axis(0).stack = true
    graphDef.axis(2).stack = true

    graphDef.plots(0).series(0).color = Some(LINE_A_COLOR)
    graphDef.plots(0).series(1).color = Some(LINE_B_COLOR)
    graphDef.plots(0).series(2).color = Some(LINE_C_COLOR)
    graphDef.plots(0).series(3).color = Some(makeTranslucent(LINE_D_COLOR))
    graphDef.plots(0).series(4).color = Some(makeTranslucent(LINE_E_COLOR))

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_stack_multiple_lines") {
    val name = prefix + "_MultiY_stack_multiple_lines.png"

    val graphDef = new GraphDef
    graphDef.title = Some("9 Lines stacked on Axis 2")

    val axis1Def = new AxisDef
    axis1Def.rightSide = true

    val axis2Def = new AxisDef
    axis2Def.rightSide = true
    axis2Def.stack = true

    graphDef.axis += (1 -> axis1Def, 2 -> axis2Def)

    val plotDef = new PlotDef
    plotDef.series = label((0 until 9).map(_ => simpleSeriesDef(100)): _*)
    plotDef.series.foreach(_.axis = Some(2))

    plotDef.series(0).color = Some(LINE_A_COLOR)
    plotDef.series(1).color = Some(LINE_B_COLOR)
    plotDef.series(2).color = Some(LINE_C_COLOR)
    plotDef.series(3).color = Some(LINE_D_COLOR)
    plotDef.series(4).color = Some(LINE_E_COLOR)
    plotDef.series(5).color = Some(Color.YELLOW)
    plotDef.series(6).color = Some(Color.CYAN)
    plotDef.series(7).color = Some(Color.BLUE)
    plotDef.series(8).color = Some(Color.LIGHT_GRAY)

    graphDef.plots = List(plotDef)
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_logarithmic") {
    val name = prefix + "_MultiY_logarithmic.png"

    val graphDef = multiYDataSet("Axis 0 Logarithmic Grid, Axis 2 Logarithmic")

    graphDef.axis(0).logarithmic = true
    graphDef.axis(2).logarithmic = true

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_logarithmic_non_primary_axis") {
    val name = prefix + "_MultiY_logarithmic_non_primary_axis.png"

    val graphDef = multiYDataSet("Axis 0 Linear Grid,  Axis 2 Logarithmic")

    graphDef.axis(2).logarithmic = true

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_hspans") {
    val name = prefix + "_MultiY_hspans.png"

    val graphDef = multiYDataSet("HSpans on all Axis")

    graphDef.plots(0).horizontalSpans = List(
      HSpan(0, 700000, 800000, Color.RED, Some("HSpan 0 on yaxis 0")),
      HSpan(1, 149.5, 150.5, Color.GREEN, Some("HSpan 1 on yaxis 1")),
      HSpan(2, 5000000, 8000000, Color.BLUE, Some("HSpan 2 on yaxis 0"))
    )

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_vspans") {
    val name = prefix + "_MultiY_vspans.png"

    val graphDef = multiYDataSet("1 VSpan on X Axis")

    val errStart = ZonedDateTime.of(2012, 1, 1, 4, 0, 0, 0, ZoneOffset.UTC).toInstant
    val errEnd = ZonedDateTime.of(2012, 1, 1, 8, 30, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.verticalSpans = List(VSpan(errStart, errEnd, Color.YELLOW, None))

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_lower") {
    val name = prefix + "_MultiY_lower.png"

    val graphDef = multiYDataSet("Lower Limit on Axis 0 & Axis 2")

    graphDef.axis(0).min = Some(300000)
    graphDef.axis(2).min = Some(15000000)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("MultiY_upper") {
    val name = prefix + "_MultiY_upper.png"

    val graphDef = multiYDataSet("Upper Limit on Axis 0 & Axis 2")

    graphDef.axis(0).max = Some(300000)
    graphDef.axis(2).max = Some(15000000)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  ignore("nearly_zero") {
    val name = prefix + "_nearly_zero.png"

    val seriesDef = new SeriesDef
    seriesDef.data = constant(java.lang.Double.MIN_VALUE)

    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  // https://github.com/Netflix/atlas/issues/119
  // TODO: fix to show label
  test("issue-119_missing_y_labels") {
    val name = prefix + "_issue-119_missing_y_labels.png"

    val seriesDef = new SeriesDef
    seriesDef.data = constant(2027)

    val plotDef = new PlotDef
    plotDef.series = label(seriesDef)

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("too_many_lines") {
    val name = prefix + "_too_many_lines.png"

    val plotDef = new PlotDef
    plotDef.series = (0 until 1024).map(simpleSeriesDef).toList

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("excessive_height") {
    val name = prefix + "_excessive_height.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(100))

    val graphDef = new GraphDef
    graphDef.height = 2048
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("excessive_width") {
    val name = prefix + "_excessive_width.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(100))

    val graphDef = new GraphDef
    graphDef.width = 2048
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  test("notices") {
    val name = prefix + "_notices.png"

    val plotDef = new PlotDef
    plotDef.series = label(simpleSeriesDef(400))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    graphDef.notices = List(
      Info("This is an information message that is shown on the graph to let the user know about " +
        "something important. It should be long enough to force the message to wrap."),
      Warning("Something bad happened and we wanted you to know."),
      Error("Something really bad happened."))

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  VisionType.values.foreach { vt =>
    test("vision_" + vt.name) {
      val name = s"${prefix}_vision_${vt.name}.png"

      val plotDef = new PlotDef
      plotDef.series = label((0 until 9).map(_ => simpleSeriesDef(100)): _*)

      val graphDef = new GraphDef
      graphDef.axis.get(0).get.stack = true
      graphDef.visionType = vt
      graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      graphDef.endTime = ZonedDateTime.of(2012, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      graphDef.plots = List(plotDef)

      val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
      graphAssertions.assertEquals(image, name, bless)
    }
  }
}

case class GraphData(
    start: Long,
    step: Long,
    legend: List[String],
    metrics: List[Map[String, String]],
    values: List[List[Double]]) {

  def toGraphDef: GraphDef = {
    val graphDef = new GraphDef
    graphDef.step = step
    graphDef.startTime = Instant.ofEpochMilli(start)
    val nbrSteps = values.length - 1
    graphDef.endTime =  graphDef.startTime.plusMillis(graphDef.step * nbrSteps)

    val seq = new ArrayTimeSeq(DsType.Gauge,
      graphDef.startTime.toEpochMilli, graphDef.step, values.flatten.toArray)

    val seriesDef = new SeriesDef
    seriesDef.data = TimeSeries(Map.empty, "test", seq)
    seriesDef.label = "0"

    val plotDef = new PlotDef
    plotDef.series = List(seriesDef)
    graphDef.plots = List(plotDef)

    graphDef
  }
}
