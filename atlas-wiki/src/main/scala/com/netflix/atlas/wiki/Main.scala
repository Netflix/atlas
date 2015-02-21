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
package com.netflix.atlas.wiki

import java.io.File
import java.io.InputStream
import java.io.OutputStream
import javax.script.ScriptEngineManager

import akka.actor.ActorSystem
import akka.actor.Props
import com.netflix.atlas.akka.RequestHandlerActor
import com.netflix.atlas.core.model.DataVocabulary
import com.netflix.atlas.core.model.FilterVocabulary
import com.netflix.atlas.core.model.MathVocabulary
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.model.StatefulVocabulary
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.model.TimeSeriesExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.util.Streams._
import com.netflix.atlas.webapi.ApiSettings
import com.netflix.atlas.webapi.LocalDatabaseActor
import com.typesafe.scalalogging.StrictLogging

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Simple script for processing the wiki docs:
 *
 * 1. Scala script embedded in the wiki that results in a string:
 *
 * ```wiki.script
 * "hello world"
 * ```
 *
 * 2. Render images for graph api:
 *
 * ```wiki.script
 * graph.image("/api/v1/graph?q=1")
 * ```
 *
 * This can also be done with a line that starts with "/api/v1/graph".
 *
 * 3. Render images without including formatted url before the image:
 *
 * ```wiki.script
 * graph.image("/api/v1/graph?q=1", false)
 * ```
 */
object Main extends StrictLogging {

  import com.netflix.atlas.core.model.ModelExtractors._

  class UseForDefaults

  type ListBuilder = scala.collection.mutable.Builder[String, List[String]]

  val system = ActorSystem("wiki")
  val db = ApiSettings.newDbInstance
  system.actorOf(Props(new LocalDatabaseActor(db)), "db")
  val webApi = system.actorOf(Props[RequestHandlerActor])

  private def eval(lines: List[String], dir: File): List[String] = {
    val engine = new ScriptEngineManager().getEngineByName("scala")
    val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
    settings.embeddedDefaults[UseForDefaults]
    assert(engine != null, s"could not find ScriptEngine for scala")
    engine.createBindings().put("graphObj", new GraphHelper(webApi, dir, "gen-images"))
    engine.eval(s"val graph = graphObj.asInstanceOf[${classOf[GraphHelper].getName}]")
    val script = lines.mkString("", "\n", "\n")
    val result = engine.eval(script)
    List(result.toString)
  }

  @scala.annotation.tailrec
  private def process(lines: List[String], output: ListBuilder, dir: File): List[String] = {
    lines match {
      case v :: vs if v.trim == "```wiki.script" =>
        val script = vs.takeWhile(_.trim != "```")
        val remainder = vs.dropWhile(_.trim != "```").tail
        output ++= eval(script, dir)
        process(remainder, output, dir)
      case v :: vs if v.trim.startsWith("/api/v1/graph") =>
        output ++= eval(List(s"""graph.image("$v")"""), dir)
        process(vs, output, dir)
      case v :: vs =>
        output += v
        process(vs, output, dir)
      case Nil =>
        output.result()
    }
  }

  private def processTemplate(f: File, output: File): Unit = {
    val template = scope(fileIn(f)) { in => lines(in).toList }
    val processed = process(template, List.newBuilder[String], new File(output, "gen-images"))
    writeFile(processed, new File(output, f.getName))
  }

  private def writeFile(lines: List[String], f: File): Unit = {
    writeFile(lines.mkString("", "\n", "\n"), f)
  }

  private def writeFile(data: String, f: File): Unit = {
    scope(fileOut(f)) { _.write(data.getBytes("UTF-8")) }
  }

  private def copyVerbatim(f: File, output: File): Unit = {
    logger.info(s"copy verbatim: $f to $output")
    copyVerbatim(fileIn(f), fileOut(new File(output, f.getName)))
  }

  private def copyVerbatim(fin: InputStream, fout: OutputStream): Unit = {
    scope(fout) { out =>
      scope(fin) { in =>
        val buf = new Array[Byte](4096)
        var length = in.read(buf)
        while (length > 0) {
          out.write(buf, 0, length)
          length = in.read(buf)
        }
      }
    }
  }

  private def copy(input: File, output: File): Unit = {
    if (!output.exists) {
      logger.info(s"creating directory: $output")
      output.mkdir()
    }
    require(output.isDirectory, s"could not find or create directory: $output")
    input.listFiles.foreach {
      case f if f.isDirectory             => copy(f, new File(output, f.getName))
      case f if f.getName.endsWith(".md") => processTemplate(f, output)
      case f                              => copyVerbatim(f, output)
    }
  }

  private def renderStack(vs: List[Any]): String = {
    val rows = vs.zipWithIndex.map { case (v, i) =>
      f"${i + 1}%5d. ${v.toString}%s"
    }
    rows.reverse.mkString("\n|")
  }

  private def zipStacks(in: List[Any], out: List[Any]): List[(Option[Any], Option[Any])] = {
    in.map(v => Some(v)).zipAll(out.map(v => Some(v)), None, None)
  }

  private def imageUri(expr: StyleExpr, params: String): String = {
    s"/api/v1/graph?q=${expr.toString}&w=200&h=100&$params"
  }

  private def renderCell(opt: Option[Any], graph: GraphHelper, params: String): String = opt match {
    case Some(v: String)           => s"<td>$v</td>"
    case Some(v: Number)           => s"<td>$v</td>"
    case Some(PresentationType(v)) => s"<td>${graph.imageHtml(imageUri(v, params))}</td>"
    case Some(v)                   => s"<td>$v</td>"
    case None                      => "<td></td>"
  }

  private def renderExample(example: String, name: String, graph: GraphHelper): String = {
    val expr = if (example.startsWith("ERROR:")) example.substring("ERROR:".length) else example

    val in = Interpreter(StyleVocabulary.allWords).execute(expr).stack

    val out = Try(Interpreter(StyleVocabulary.allWords).execute(s"$expr,:$name")) match {
      case Success(c) => c.stack
      case Failure(t) => List(s":bangbang: <b>${t.getClass.getSimpleName}:</b> ${t.getMessage}")
    }

    val params = if (name.startsWith("cf-")) "step=5m" else ""

    val buf = new StringBuilder
    buf.append(s"### `$example,:$name`")
    buf.append("<table><thead><th>Pos</th><th>Input</th><th>Output</th></thead><tbody>")
    zipStacks(in, out).zipWithIndex.foreach { case ((i, o), p) =>
      buf.append(s"<tr>\n<td>$p</td>\n")
         .append(s"${renderCell(i, graph, params)}\n")
         .append(s"${renderCell(o, graph, params)}\n<s/tr>")
    }
    buf.append("</tbody></table>\n")
    buf.toString()
  }

  private def renderWord(w: Word, graph: GraphHelper): String = {
    s"""
       |## Signature
       |
       |`${w.signature}`
       |
       |## Summary
       |
       |${w.summary}
       |
       |## Examples
       |
       |${w.examples.map(ex => renderExample(ex, w.name, graph)).mkString("\n|\n|")}
    """.stripMargin.trim
  }

  private def generateStackLangRef(output: File): Unit = {
    val dir = new File(output, "stacklang")
    dir.mkdirs()

    val graph = new GraphHelper(webApi, new File(dir, "gen-images"), "stacklang/gen-images")

    val vocabs = List(
      StandardVocabulary,
      QueryVocabulary,
      DataVocabulary,
      MathVocabulary,
      StatefulVocabulary,
      StyleVocabulary
    )

    val sidebar = new StringBuilder
    sidebar.append("###[Home](Home) > Stack Language Reference\n")

    vocabs.foreach { vocab =>
      sidebar.append(s"\n**${vocab.name}**\n")
      vocab.words.foreach { w =>
        // Using unicode hyphen is a hack to get around:
        // https://github.com/github/markup/issues/345
        val fname = s"${vocab.name}-${w.name.replace('-', '\u2010')}"
        sidebar.append(s"* [${w.name}]($fname)\n")
        val f = new File(dir, s"$fname.md")
        writeFile(renderWord(w, graph), f)
      }
    }

    writeFile(sidebar.toString(), new File(dir, "_Sidebar.md"))
  }

  def main(args: Array[String]): Unit = {
    try {
      if (args.length != 2) {
        System.err.println("Usage: Main <input-dir> <output-dir>")
        System.exit(1)
      }

      val input = new File(args(0))
      require(input.isDirectory, s"input-dir is not a directory: $input")

      val output = new File(args(1))
      output.mkdirs()
      require(output.isDirectory, s"could not find or create output directry: $output")

      copy(input, output)

      generateStackLangRef(output)
    } finally {
      system.shutdown()
    }
  }
}
