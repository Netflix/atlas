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
package com.netflix.atlas.eval.graph

import java.awt.Color
import java.io.ByteArrayOutputStream
import java.time.Duration
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.Uri
import com.netflix.atlas.chart.Colors
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.Layout
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.MessageDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.TickLabelMode
import com.netflix.atlas.chart.model.VisionType
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Expr
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.model.ResultSet
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.SummaryStats
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Features
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.core.util.UnitPrefix
import com.netflix.atlas.eval.util.IdParamSanitizer
import com.netflix.atlas.pekko.Cors
import com.typesafe.config.Config

import java.util.Locale
import scala.util.Try

case class Grapher(settings: DefaultSettings) {

  import Grapher.*

  /**
    * Create a graph config from a request object. This will look at the URI and try to
    * extract some context from the headers.
    */
  def toGraphConfig(request: HttpRequest): GraphConfig = {
    val config = rewriteBasedOnHost(request, toGraphConfig(request.uri))

    // Check if the request is coming from a browser
    val agent = request.headers
      .find(_.is("user-agent"))
      .map(_.value())
      .getOrElse("unknown")
    val isBrowser = settings.browserAgentPattern.matcher(agent).find()

    // Only look at headers if the id is not explicitly set on the URI
    if (config.id == "default") {
      val origin = request.headers
        .find(_.is("origin"))
        .flatMap(h => Cors.normalizedOrigin(h.value()))
        .getOrElse("default")
      config.copy(isBrowser = isBrowser, id = IdParamSanitizer.sanitize(origin))
    } else {
      config.copy(isBrowser = isBrowser)
    }
  }

  private def rewriteBasedOnHost(request: HttpRequest, config: GraphConfig): GraphConfig = {
    request.headers
      .find(_.is("host"))
      .map(_.value())
      .fold(config) { host =>
        val newExprs = config.parsedQuery.map { exprs =>
          settings.hostRewriter.rewrite(host, exprs)
        }
        val query = if (newExprs.isFailure) config.query else newExprs.get.mkString(",")
        config.copy(query = query, parsedQuery = newExprs)
      }
  }

  /**
    * Hints parameter is a comma separated set of strings that will be passed on to the
    * rendering engine to optionally tune behavior.
    */
  private def processHints(hints: Option[String]): Set[String] = {
    hints.toSeq
      .flatMap(_.split(','))
      .map(_.trim)
      .filterNot(_.isEmpty)
      .toSet
  }

  /**
    * Create a graph config from an Atlas URI.
    */
  def toGraphConfig(uri: Uri): GraphConfig = {
    val params = uri.query()
    val id = IdParamSanitizer.sanitize(params.get("id").getOrElse("default"))

    val features = params
      .get("features")
      .map(v => Features.valueOf(v.toUpperCase(Locale.US)))
      .getOrElse(Features.STABLE)

    import com.netflix.atlas.chart.GraphConstants.*
    val axes = (0 until MaxYAxis).map(i => i -> newAxis(params, i)).toMap

    val vision = params.get("vision").map(v => VisionType.valueOf(v))

    val theme = params.get("theme").getOrElse(settings.theme)
    val palette = params.get("palette").getOrElse(settings.primaryPalette(theme))

    val flags = ImageFlags(
      title = params.get("title").filter(_ != ""),
      width = params.get("w").fold(settings.width)(_.toInt),
      height = params.get("h").fold(settings.height)(_.toInt),
      zoom = params.get("zoom").fold(1.0)(_.toDouble),
      axes = axes,
      axisPerLine = params.get("axis_per_line").contains("1"),
      showLegend = !params.get("no_legend").contains("1"),
      showLegendStats = !params.get("no_legend_stats").contains("1"),
      showOnlyGraph = params.get("only_graph").contains("1"),
      vision = vision.getOrElse(VisionType.normal),
      palette = palette,
      theme = theme,
      layout = Layout.create(params.get("layout").getOrElse("canvas")),
      hints = processHints(params.get("hints"))
    )

    val q = params.get("q")
    if (q.isEmpty) {
      throw new IllegalArgumentException("missing required parameter 'q'")
    }

    // Global common query to apply to all expressions. It will apply across freeze operations
    // and thus provides a global context.
    val cq = params.get("cq").fold[Any => Any](v => v) { q =>
      val query = parseQuery(q)
      v => applyCQ(v, query)
    }

    val timezones = params.getAll("tz").reverse
    val parsedQuery = Try {
      val vars = Map("tz" -> GraphConfig.getTimeZoneIds(settings, timezones).head)
      val exprs = settings.interpreter
        .execute(q.get, vars, features)
        .stack
        .reverse
        .map(cq)
        .flatMap {
          case ModelExtractors.PresentationType(s) =>
            s.perOffset
          case v =>
            val tpe = v.getClass.getSimpleName
            throw new IllegalArgumentException(s"expecting time series expr, found $tpe '$v'")
        }
      if (settings.simpleLegendsEnabled)
        SimpleLegends.generate(exprs)
      else
        exprs
    }

    GraphConfig(
      settings = settings,
      query = q.get,
      parsedQuery = parsedQuery,
      start = params.get("s"),
      end = params.get("e"),
      timezones = timezones,
      step = params.get("step"),
      flags = flags,
      format = params.get("format").getOrElse("png"),
      id = id,
      features = features,
      isBrowser = false,
      isAllowedFromBrowser = true,
      uri = uri.toString
    )
  }

  private def applyCQ(value: Any, cq: Query): Any = value match {
    case expr: Expr =>
      expr.rewrite {
        case q: Query => q.and(cq)
      }
    case v =>
      v
  }

  private def getAxisParam(params: Uri.Query, k: String, id: Int): Option[String] = {
    params.get(s"$k.$id").orElse(params.get(k))
  }

  private def newAxis(params: Uri.Query, id: Int): Axis = {

    // Prefer the scale parameter if present. If not, then fallback to look at
    // the boolean `o` parameter for backwards compatibility.
    val scale = getAxisParam(params, "scale", id).orElse {
      if (getAxisParam(params, "o", id).contains("1")) Some("log") else None
    }
    Axis(
      upper = getAxisParam(params, "u", id),
      lower = getAxisParam(params, "l", id),
      scale = scale,
      stack = getAxisParam(params, "stack", id).contains("1"),
      ylabel = getAxisParam(params, "ylabel", id).filter(_ != ""),
      tickLabels = getAxisParam(params, "tick_labels", id),
      palette = params.get(s"palette.$id"),
      sort = getAxisParam(params, "sort", id),
      order = getAxisParam(params, "order", id),
      heatmapScale = getAxisParam(params, "heatmap_scale", id),
      heatmapUpper = getAxisParam(params, "heatmap_u", id),
      heatmapLower = getAxisParam(params, "heatmap_l", id),
      heatmapPalette = getAxisParam(params, "heatmap_palette", id),
      heatmapLabel = getAxisParam(params, "heatmap_label", id)
    )
  }

  /**
    * Evaluate the expressions and render a chart using the config from the uri and the
    * specified data.
    */
  def evalAndRender(uri: Uri, db: Database): Result = evalAndRender(toGraphConfig(uri), db)

  /**
    * Evaluate the expressions and render a chart using the config from the uri and the
    * specified data.
    */
  def evalAndRender(uri: Uri, data: List[TimeSeries]): Result =
    evalAndRender(toGraphConfig(uri), data)

  /**
    * Evaluate the expressions and render a chart using the config from the uri and the
    * specified data. The data must have already been pre-processed to only include relevant
    * results for each DataExpr. It is up to the user to ensure the DataExprs in the map
    * match those that will be extracted from the uri.
    */
  def evalAndRender(uri: Uri, data: DataMap): Result = evalAndRender(toGraphConfig(uri), data)

  /** Evaluate the expressions and render a chart using the specified config and data. */
  def evalAndRender(config: GraphConfig, db: Database): Result = {
    val dataExprs = config.exprs.flatMap(_.expr.dataExprs).distinct
    val result = dataExprs.map(expr => expr -> db.execute(config.evalContext, expr)).toMap
    evalAndRender(config, result)
  }

  /** Evaluate the expressions and render a chart using the specified config and data. */
  def evalAndRender(config: GraphConfig, data: List[TimeSeries]): Result = {
    val dataExprs = config.exprs.flatMap(_.expr.dataExprs).distinct
    val result = dataExprs.map(expr => expr -> eval(config.evalContext, expr, data)).toMap
    evalAndRender(config, result)
  }

  private def eval(
    context: EvalContext,
    expr: DataExpr,
    data: List[TimeSeries]
  ): List[TimeSeries] = {
    val matches = data.filter(t => expr.query.matches(t.tags))
    val offset = expr.offset.toMillis
    if (offset == 0) expr.eval(context, matches).data
    else {
      val offsetContext = context.withOffset(expr.offset.toMillis)
      expr.eval(offsetContext, matches).data.map { t =>
        t.offset(offset)
      }
    }
  }

  /**
    * Evaluate the expressions and render a chart using the specified config and data. The data
    * must have already been pre-processed to only include relevant results for each DataExpr. It
    * is up to the user to ensure the DataExprs in the map match those that will be extracted from
    * the config.
    */
  def evalAndRender(config: GraphConfig, data: DataMap): Result = {
    val graphDef = create(config, _.expr.eval(config.evalContext, data))
    val baos = new ByteArrayOutputStream
    config.engine.write(graphDef, baos)
    Result(config, baos.toByteArray)
  }

  /**
    * Render a chart using the config from the uri and the specified data. The data must
    * have already been pre-processed to only include relevant results for each DataExpr.
    * It is up to the user to ensure the DataExprs in the map match those that will be
    * extracted from the uri.
    */
  def render(uri: Uri, data: StyleMap): Result = render(toGraphConfig(uri), data)

  /**
    * Render a chart using the specified config and data. It is up to the user to ensure the
    * StyleExprs in the map match those that will be extracted from the config.
    */
  def render(config: GraphConfig, data: StyleMap): Result = {
    val graphDef = create(config, s => ResultSet(s.expr, data.getOrElse(s, Nil)))
    val baos = new ByteArrayOutputStream
    config.engine.write(graphDef, baos)
    Result(config, baos.toByteArray)
  }

  /** Create a new graph definition based on the specified config and data. */
  def create(config: GraphConfig, eval: StyleExpr => ResultSet): GraphDef = {

    val warnings = List.newBuilder[String]

    val plotExprs = config.exprs.groupBy(_.axis.getOrElse(0))
    val multiY = plotExprs.size > 1 && !GraphDef.ambiguousMultiY(config.flags.hints)

    val palette = newPalette(config.flags.palette)
    val shiftPalette = newPalette(settings.offsetPalette(config.flags.theme))

    val start = config.startMillis
    val end = config.endMillis

    val plots = plotExprs.toList.sortWith(_._1 < _._1).map {
      case (yaxis, exprs) =>
        val axisCfg = config.flags.getAxis(yaxis)
        val dfltStyle = if (axisCfg.stack) LineStyle.STACK else LineStyle.LINE

        val statFormatter = axisCfg.tickLabelMode match {
          case TickLabelMode.BINARY =>
            (v: Double) => UnitPrefix.binary(v).format(v)
          case _ =>
            (v: Double) => UnitPrefix.decimal(v).format(v)
        }

        val axisPalette = axisCfg.palette.fold(palette) { v =>
          newPalette(v)
        }

        var messages = List.empty[String]
        var heatmapColor: Color = null
        val lines = exprs.flatMap { s =>
          val result = eval(s)

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
            val stats = SummaryStats(t.data, start, end)
            val offset = Strings.toString(Duration.ofMillis(s.offset))
            val outputTags = t.tags + (TagKey.offset -> offset)
            // Additional stats can be used for substitutions, but should not be included
            // as part of the output tag map
            val legendTags = outputTags ++ stats.tags(statFormatter)
            val newT = t.withTags(outputTags)
            newT.withLabel(s.legend(newT.label, legendTags)) -> stats
          }

          val palette = s.palette.map(newPalette).getOrElse {
            s.color
              .map { c =>
                val color = settings.resolveColor(config.flags.theme, c)
                val p = Palette.singleColor(color).iterator
                (_: String) => p.next()
              }
              .getOrElse {
                if (s.offset > 0L) shiftPalette else axisPalette
              }
          }

          val lineDefs = labelledTS.sortWith(_._1.label < _._1.label).map {
            case (t, stats) =>
              val lineStyle = s.lineStyle.fold(dfltStyle)(LineStyle.parse)
              val color = s.color.fold {
                val c = lineStyle match {
                  case LineStyle.HEATMAP =>
                    if (axisCfg.heatmapPalette.nonEmpty) {
                      // Don't consume a color if the the global heatmap palette is configured.
                      // Just set it to something.
                      if (heatmapColor == null) heatmapColor = Color.BLACK
                    } else {
                      if (heatmapColor == null) heatmapColor = palette(s"heatmap$yaxis")
                    }
                    heatmapColor
                  case _ => palette(t.label)
                }
                // Alpha setting if present will set the alpha value for the color automatically
                // assigned by the palette. If using an explicit color it will have no effect as the
                // alpha can be set directly using an ARGB hex format for the color.
                s.alpha.fold(c)(a => Colors.withAlpha(c, a))
              } { c =>
                settings.resolveColor(config.flags.theme, c)
              }

              LineDef(
                data = t,
                query = Some(s.toString),
                groupByKeys = s.expr.finalGrouping,
                color = color,
                lineStyle = s.lineStyle.fold(dfltStyle)(LineStyle.parse),
                lineWidth = s.lineWidth,
                legendStats = stats
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

    config.newGraphDef(plots, warnings.result())
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
          (a, b) => compare(useDescending, a.data.label, b.data.label)
        case "min" =>
          (a, b) => compare(useDescending, a.legendStats.min, b.legendStats.min)
        case "max" =>
          (a, b) => compare(useDescending, a.legendStats.max, b.legendStats.max)
        case "avg" =>
          (a, b) => compare(useDescending, a.legendStats.avg, b.legendStats.avg)
        case "count" =>
          (a, b) => compare(useDescending, a.legendStats.count, b.legendStats.count)
        case "total" =>
          (a, b) => compare(useDescending, a.legendStats.total, b.legendStats.total)
        case "last" =>
          (a, b) => compare(useDescending, a.legendStats.last, b.legendStats.last)
        case order =>
          warnings += s"Invalid sort mode '$order'. Using default of 'legend'."
          (a, b) => compare(useDescending, a.data.label, b.data.label)
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

object Grapher {

  def apply(root: Config): Grapher = Grapher(DefaultSettings(root))

  /**
    * Rendered graph result.
    *
    * @param config
    *     The config used to generate the graph.
    * @param data
    *     Rendered data. The format of this data will depend on the config settings
    *     for the graph.
    */
  case class Result(config: GraphConfig, data: Array[Byte]) {
    def dataString: String = new String(data, "UTF-8")
  }

  private val queryInterpreter = Interpreter(QueryVocabulary.allWords)

  private def parseQuery(str: String): Query = {
    queryInterpreter.execute(str).stack match {
      case (q: Query) :: Nil => q
      case _                 => throw new IllegalStateException(s"invalid cq parameter: $str")
    }
  }
}
