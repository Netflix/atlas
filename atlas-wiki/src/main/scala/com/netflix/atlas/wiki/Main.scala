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
import com.netflix.atlas.core.util.Streams._
import com.netflix.atlas.webapi.ApiSettings
import com.netflix.atlas.webapi.LocalDatabaseActor
import com.typesafe.scalalogging.StrictLogging

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
    engine.createBindings().put("graphObj", new GraphHelper(webApi, dir))
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
    } finally {
      system.shutdown()
    }
  }
}
