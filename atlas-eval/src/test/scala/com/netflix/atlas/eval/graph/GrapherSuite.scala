/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.eval.graph

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.Host
import com.netflix.atlas.chart.util.GraphAssertions
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.core.db.StaticDatabase
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.Json
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class GrapherSuite extends FunSuite {

  private val bless = false

  // SBT working directory gets updated with fork to be the dir for the project
  private val baseDir = SrcPath.forProject("atlas-eval")
  private val goldenDir = s"$baseDir/src/test/resources/graph/${getClass.getSimpleName}"
  private val targetDir = s"$baseDir/target/${getClass.getSimpleName}"

  private val graphAssertions =
    new GraphAssertions(goldenDir, targetDir, (a, b) => assertEquals(a, b))

  private val db = StaticDatabase.demo
  private val grapher = Grapher(ConfigFactory.load())

  override def afterAll(): Unit = {
    graphAssertions.generateReport(getClass)
  }

  def imageTest(name: String)(uri: => String): Unit = {
    test(name) {
      val fname = Strings.zeroPad(Hash.sha1bytes(name), 40).substring(0, 8) + ".png"
      val result = grapher.evalAndRender(Uri(uri), db)
      val image = PngImage(result.data)
      graphAssertions.assertEquals(image, fname, bless)
    }
  }

  imageTest("simple expr") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum"
  }

  imageTest("timezone: UTC") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,:sum&tz=UTC"
  }

  imageTest("timezone: US/Pacific") {
    "/api/v1/graph?e=2012-01-01T00:00&s=e-1d&q=name,sps,:eq,:sum&tz=US/Pacific"
  }

  imageTest("timezone: UTC and US/Pacific") {
    "/api/v1/graph?e=2012-01-01T00:00&s=e-1d&q=name,sps,:eq,:sum&tz=UTC&tz=US/Pacific"
  }

  imageTest("line colors") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,:sum,:dup,1000,:add,f00,:color"
  }

  imageTest("legend text") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,:sum,starts+per+second,:legend"
  }

  imageTest("legend text using exact match var") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,:sum,$name,:legend"
  }

  imageTest("legend text using group by var") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend"
  }

  imageTest("legend text using atlas.offset") {
    "/api/v1/graph?e=2012-01-01T00:00&q=" +
      "(,0h,1d,1w,),(," +
      "name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum," +
      ":swap,:offset," +
      "$(name)+(offset%3D$(atlas.offset)),:legend," +
      "),:each"
  }

  imageTest("group by and stack") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend&stack=1"
  }

  imageTest("group by, pct, and stack") {
    "/api/v1/graph?e=2012-01-01T00:00&q=" +
      "name,sps,:eq,(,nf.cluster,),:by,:pct,$nf.cluster,:legend" +
      "&stack=1"
  }

  imageTest("upper and lower bounds") {
    "/api/v1/graph?e=2012-01-01T00:00&q=" +
      "name,sps,:eq,nf.cluster,nccp-.*,:re,:and,:sum,(,nf.cluster,),:by,$nf.cluster,:legend" +
      "&stack=1&l=0&u=50e3"
  }

  private val baseAxisScaleQuery = "/api/v1/graph?e=2012-01-01T00:00" +
    "&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and," +
    ":dup,:sum," +
    ":swap,:count," +
    ":over,:over,:div,average,:legend," +
    ":rot,sum,:legend," +
    ":rot,count,:legend"

  imageTest("axis using legacy o=1 param") {
    baseAxisScaleQuery + "&o=1"
  }

  imageTest("axis using log scale") {
    baseAxisScaleQuery + "&scale=log"
  }

  imageTest("axis using pow2 scale") {
    baseAxisScaleQuery + "&scale=pow2"
  }

  imageTest("axis using sqrt scale") {
    baseAxisScaleQuery + "&scale=sqrt"
  }

  imageTest("axis using linear scale") {
    baseAxisScaleQuery + "&scale=linear"
  }

  imageTest("axis using legacy scale param overides o param") {
    baseAxisScaleQuery + "&scale=linear&o=1"
  }

  imageTest("axis using binary tick labels") {
    baseAxisScaleQuery + "&tick_labels=binary"
  }

  imageTest("axis using duration tick labels") {
    baseAxisScaleQuery + "&tick_labels=duration"
  }

  imageTest("axis using duration tick labels small values") {
    "/api/v1/graph?e=2012-01-01T00:00&q=1.0e-12,1.0e-9&tick_labels=duration"
  }

  imageTest("axis with offset ticks") {
    "/api/v1/graph?e=2012-01-01T00:00&q=1.0e9,1.0e9,1,:add"
  }

  private val baseStatAxisScaleQuery = "/api/v1/graph?e=2012-01-01T00:00&s=e-2d" +
    "&q=name,sps,:eq,:sum,:dup,:dup,min,:stat,:sub,:swap,max,:stat,0.5,:mul,:sub"

  imageTest("stat query with axis using log scale") {
    baseStatAxisScaleQuery + "&scale=log"
  }

  imageTest("stat query with axis using pow2 scale") {
    baseStatAxisScaleQuery + "&scale=pow2"
  }

  imageTest("stat query with axis using sqrt scale") {
    baseStatAxisScaleQuery + "&scale=sqrt"
  }

  imageTest("stat query with axis using linear scale") {
    baseStatAxisScaleQuery + "&scale=linear"
  }

  imageTest("average") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:avg," +
      "avg+sps+for+silverlight,:legend"
  }

  imageTest("title and legends") {
    "/api/v1/graph?s=e-1w&e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:avg,avg+sps+for+silverlight,:legend" +
      "&no_legend=1&title=Silverlight+SPS&ylabel=Starts+per+second"
  }

  imageTest("line on area") {
    // Area must be drawn first or line will be covered up
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=nf.cluster,nccp-silverlight,:eq,name,sps,:eq,:and,:sum,:dup,10000,:add,:area,:swap"
  }

  imageTest("stack, areas, and lines") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,:sum,:area," +
      "name,sps,:eq,nf.cluster,nccp-ps3,:eq,:and,:sum,:stack," +
      "name,sps,:eq,:avg,100,:mul"
  }

  imageTest("transparency as part of color") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=nf.cluster,nccp-silverlight,:eq,name,sps,:eq,:and,:sum," +
      ":dup,10000,:add,:area,400000ff,:color"
  }

  imageTest("transparency using alpha") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=nf.cluster,nccp-silverlight,:eq,name,sps,:eq,:and,:sum," +
      ":dup,10000,:add,:area,40,:alpha"
  }

  imageTest("DES: delta as area") {
    "/api/v1/graph?tz=UTC&e=2012-01-01T12:00&s=e-12h&w=750&h=150&l=0" +
      "&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum," +
      ":dup,:des-simple,0.9,:mul," +
      ":2over,:sub,:abs,:area,40,:alpha," +
      ":rot,$name,:legend," +
      ":rot,prediction,:legend," +
      ":rot,delta,:legend"
  }

  imageTest("DES: vspan showing trigger") {
    "/api/v1/graph?tz=UTC&e=2012-01-01T12:00&s=e-12h&w=750&h=150&l=0" +
      "&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum" +
      ",:dup,:des-simple,0.9,:mul," +
      ":2over,:lt," +
      ":rot,$name,:legend," +
      ":rot,prediction,:legend," +
      ":rot,:vspan,60,:alpha,alert+triggered,:legend"
  }

  imageTest("smoothing using DES") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,type,high-noise,:eq,:and,:sum," +
      "10,0.145,0.01,:des" +
      "&w=750&h=100&no_legend=1&s=e-12h"
  }

  imageTest("smoothing using trend") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-ps3,:eq,:and,:avg," +
      ":dup,:dup,:dup,5m,:trend,100,:add,5m+trend,:legend," +
      ":rot,10m,:trend,200,:add,10m+trend,:legend," +
      ":rot,20m,:trend,300,:add,20m+trend,:legend," +
      ":rot,original+line,:legend,:-rot" +
      "&w=750&h=300&s=e-12h"
  }

  imageTest("smoothing using step 5m") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,type,high-noise,:eq,:and,:sum" +
      "&step=PT5M&w=750&h=100&no_legend=1&s=e-12h"
  }

  imageTest("smoothing using step 20m") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silverlight,:eq,:and,type,high-noise,:eq,:and,:sum" +
      "&step=PT20M&w=750&h=100&no_legend=1&s=e-12h"
  }

  imageTest("math with time shifts") {
    "/api/v1/graph?e=2012-01-01T12:00&s=e-12h&tz=UTC" +
      "&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum," +
      ":dup,1w,:offset,:sub,:area,delta+week+over+week,:legend" +
      "&h=150&w=750"
  }

  imageTest("average over last 3w") {
    "/api/v1/graph?e=2012-01-01T12:00&s=e-12h&tz=UTC" +
      "&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum," +
      ":dup,1w,:offset,:over,2w,:offset,:add,:over,3w,:offset,:add,3,:div," +
      ":2over,:swap,:over,:sub,:abs,:swap,:div,100,:mul," +
      ":rot,requestsPerSecond,:legend," +
      ":rot,average+for+previous+3+weeks,:legend," +
      ":rot,:area,40,:alpha,percent+delta,:legend" +
      "&h=150&w=750"
  }

  imageTest("multi-Y") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=nf.node,alert1,:eq,:sum,nf.node,alert1,:eq,:count,1,:axis" +
      "&ylabel.0=Axis%200&ylabel.1=Axis%201"
  }

  imageTest("significant time boundaries and tz=US/Pacific") {
    "/api/v1/graph?q=name,sps,:eq,:sum&s=e-2d&e=2015-06-17T13:13&no_legend=1&tz=US/Pacific"
  }

  imageTest("significant time boundaries and tz=UTC") {
    "/api/v1/graph?q=name,sps,:eq,:sum&s=e-2d&e=2015-06-17T13:13&no_legend=1&tz=UTC"
  }

  imageTest("daylight savings time transition") {
    "/api/v1/graph?q=name,sps,:eq,:sum&s=e-4d&e=2015-03-10T13:13&no_legend=1&tz=US/Pacific"
  }

  imageTest("daylight savings time transition, with 1d step") {
    "/api/v1/graph?q=name,sps,:eq,:sum&s=e-1d&e=2015-03-10T13:13&no_legend=1&tz=US/Pacific&step=1d"
  }

  imageTest("daylight savings time transition, US/Pacific and UTC") {
    "/api/v1/graph?q=name,sps,:eq,:sum&s=e-4d&e=2015-03-10T13:13&no_legend=1&tz=US/Pacific&tz=UTC"
  }

  imageTest("daylight savings time transition, UTC, Pacific, and Eastern") {
    "/api/v1/graph?q=name,sps,:eq,:sum&s=e-4d&e=2015-03-10T13:13&no_legend=1" +
      "&tz=UTC&tz=US/Pacific&tz=US/Eastern&step=1d"
  }

  imageTest("vision flag") {
    "/api/v1/graph?s=e-1d&e=2015-03-10T13:13" +
      "&q=(,1,2,3,4,5,6,7,8,9,),(,nf.cluster,nccp-silverlight,:eq,name,sps,:eq,:and,:sum,:swap,:legend,),:each" +
      "&vision=protanopia&no_legend=1&stack=1"
  }

  imageTest("z-order of stacked lines") {
    "/api/v1/graph" +
      "?q=t,name,sps,:eq,:sum,:set,t,:get,:stack,t,:get,1.1,:mul,6h,:offset,t,:get,4,:div,:stack" +
      "&s=e-2d&e=2015-03-10T13:13"
  }

  imageTest("issue-1146") {
    "/api/v1/graph?s=e-1d&e=2012-01-01T00:00&q=name,sps,:eq,:sum,:dup,30m,:offset,4,:axis&l.4=0"
  }

  imageTest("expr scoped palette") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,reds,:palette,:stack,name,sps,:eq,2,:lw,50e3,45e3"
  }

  imageTest("expr scoped palette in the middle") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,4,:lw,name,sps,:eq,(,nf.cluster,),:by,reds,:palette,:stack,50e3,45e3"
  }

  imageTest("multiple expressions with scoped palettes") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,:dup,reds,:palette,:stack,:swap,greens,:palette,:stack"
  }

  imageTest("expr palette overrides axis param") {
    "/api/v1/graph?e=2012-01-01T00:00&palette=greens" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,reds,:palette,:stack"
  }

  imageTest("expr palette then color") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,reds,:palette,00f,:color,:stack"
  }

  imageTest("color then expr palette") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,00f,:color,reds,:palette,:stack"
  }

  imageTest("expr palette with alpha") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=50e3,4,:lw,name,sps,:eq,(,nf.cluster,),:by,reds,:palette,40,:alpha,:stack"
  }

  imageTest("expr scoped hashed palette") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,hash:reds,:palette,:stack"
  }

  imageTest("substitute max stat in legend") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$atlas.max+is+max,:legend"
  }

  imageTest("substitute max stat in legend honors label mode") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$atlas.max+is+max,:legend&tick_labels=binary"
  }

  imageTest("empty legend string") {
    "/api/v1/graph?e=2012-01-01T00:00&q=name,sps,:eq,(,nf.cluster,),:by,$(),:legend"
  }

  imageTest("substitutions for ylabel, name present, cluster missing") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silver,:lt,:and,(,nf.cluster,),:by" +
      "&ylabel=$name+$nf.cluster"
  }

  imageTest("substitutions for ylabel, name present, cluster present") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silver,:lt,:and,(,nf.cluster,),:by" +
      "&ylabel=$name+$nf.cluster&axis_per_line=1"
  }

  imageTest("substitutions for title, name present, cluster missing") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-silver,:lt,:and,(,nf.cluster,),:by" +
      "&title=$name+$nf.cluster&axis_per_line=1"
  }

  imageTest("using dark24 palette") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend" +
      "&palette=dark24"
  }

  imageTest("using light24 palette") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend" +
      "&palette=light24"
  }

  imageTest("using dark theme") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend" +
      "&theme=dark"
  }

  imageTest("using dark theme with multi-Y") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend,42,1,:axis" +
      "&theme=dark"
  }

  imageTest("using dark theme with offset") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,:dup,1w,:offset" +
      "&theme=dark"
  }

  imageTest("ambiguous-multi-y with axis per line") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,nf.cluster,nccp-p,:re,:and,(,nf.cluster,),:by" +
      "&axis_per_line=1&hints=ambiguous-multi-y"
  }

  imageTest("ambiguous-multi-y explicit") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,:sum,:dup,2,:div,1,:axis" +
      "&hints=ambiguous-multi-y"
  }

  imageTest("topk") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:topk&features=unstable"
  }

  imageTest("topk-others-min") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:topk-others-min&features=unstable"
  }

  imageTest("topk-others-max") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:topk-others-max&features=unstable"
  }

  imageTest("topk-others-sum") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:topk-others-sum&features=unstable"
  }

  imageTest("topk-others-avg") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:topk-others-avg&features=unstable"
  }

  imageTest("bottomk") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:bottomk&features=unstable"
  }

  imageTest("bottomk-others-min") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:bottomk-others-min&features=unstable"
  }

  imageTest("bottomk-others-max") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:bottomk-others-max&features=unstable"
  }

  imageTest("bottomk-others-sum") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:bottomk-others-sum&features=unstable"
  }

  imageTest("bottomk-others-avg") {
    "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,max,2,:bottomk-others-avg&features=unstable"
  }

  test("invalid stuff on stack") {
    val uri = "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,(,nf.cluster,),:by,foo"
    val e = intercept[IllegalArgumentException] {
      grapher.toGraphConfig(Uri(uri)).parsedQuery.get
    }
    assertEquals(e.getMessage, "expecting time series expr, found String 'foo'")
  }

  def renderTest(name: String)(uri: => String): Unit = {
    test(name) {
      val fname = Strings.zeroPad(Hash.sha1bytes(name), 40).substring(0, 8) + ".png"
      val config = grapher.toGraphConfig(Uri(uri))
      val styleData = config.exprs.map { styleExpr =>
        val dataResults = styleExpr.expr.dataExprs.distinct.map { dataExpr =>
          dataExpr -> db.execute(config.evalContext, dataExpr)
        }.toMap
        styleExpr -> styleExpr.expr.eval(config.evalContext, dataResults).data
      }.toMap
      val result = grapher.render(Uri(uri), styleData)
      val image = PngImage(result.data)
      graphAssertions.assertEquals(image, fname, bless)
    }
  }

  renderTest("rendering with pre-evaluated data set, legends") {
    "/api/v1/graph?e=2012-01-01T00:00" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend,10e3,threshold,:legend,3,:lw"
  }

  renderTest("rendering with pre-evaluated data set, multi-y") {
    "/api/v1/graph?e=2012-01-01T00:00&u.1=56e3" +
      "&q=name,sps,:eq,(,nf.cluster,),:by,$nf.cluster,:legend,:stack,20e3,1,:axis,:area,50,:alpha"
  }

  test("stat vars are not included in tag map") {
    val uri = "/api/v1/graph?e=2012-01-01&q=name,sps,:eq,nf.app,nccp,:eq,:and,:sum&format=json"
    val json = grapher.evalAndRender(Uri(uri), db).data
    val response = Json.decode[GrapherSuite.GraphResponse](json)
    List("avg", "last", "max", "min", "total").foreach { stat =>
      response.metrics.foreach { m =>
        assert(!m.contains(s"atlas.$stat"))
      }
    }
  }

  private def mkRequest(host: String, expr: String = "name,sps,:eq,:sum"): HttpRequest = {
    HttpRequest(uri = Uri(s"/api/v1/graph?q=$expr"), headers = List(Host(host)))
  }

  test("host rewrite: no match") {
    val request = mkRequest("foo.example.com")
    val config = grapher.toGraphConfig(request)
    assertEquals(config, grapher.toGraphConfig(request.uri))
  }

  test("host rewrite: match") {
    val request = mkRequest("foo.us-east-1.example.com")
    val config = grapher.toGraphConfig(request)
    assertNotEquals(config, grapher.toGraphConfig(request.uri))
    assert(config.query.contains("region,us-east-1,:eq"))
    assert(config.parsedQuery.get.forall(_.toString.contains("region,us-east-1,:eq")))
  }

  test("host rewrite: bad query") {
    val request = mkRequest("foo.us-east-1.example.com", "a,b,:foo")
    val config = grapher.toGraphConfig(request)
    assertEquals(config.query, "a,b,:foo")
    assert(config.parsedQuery.isFailure)
  }
}

object GrapherSuite {

  case class GraphResponse(
    start: Long,
    step: Long,
    legend: List[String],
    metrics: List[Map[String, String]],
    values: Array[Array[Double]],
    notices: List[String],
    explain: Map[String, Long]
  )
}
