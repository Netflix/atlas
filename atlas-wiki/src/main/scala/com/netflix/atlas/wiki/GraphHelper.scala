/*
 * Copyright 2014-2019 Netflix, Inc.
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
import java.net.URI

import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.Uri
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonFactoryBuilder
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.fasterxml.jackson.core.json.JsonWriteFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Streams._
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.graph.Grapher
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

class GraphHelper(db: Database, dir: File, path: String) extends StrictLogging {

  private val grapher = Grapher(ConfigFactory.load())

  private val baseUri = s"https://raw.githubusercontent.com/wiki/Netflix/atlas/$path"

  private val wordLinkBaseUri = "https://github.com/Netflix/atlas/wiki"
  private val interpreter = Interpreter(StyleVocabulary.allWords)
  private val vocabularies = StyleVocabulary :: StyleVocabulary.dependencies

  override def toString: String = s"GraphHelper($dir, $path)"

  private def prettyPrint(json: String): String = {
    val factory = JsonFactory
      .builder()
      .asInstanceOf[JsonFactoryBuilder]
      .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
      .disable(JsonWriteFeature.WRITE_NAN_AS_STRINGS)
      .build()
    val mapper = new ObjectMapper(factory)
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
    mapper.writeValueAsString(mapper.readTree(json))
  }

  def image(uri: String, showQuery: Boolean = true): String = {
    logger.info(s"creating image for: $uri")
    val fname = imageFileName(uri)
    val res = grapher.evalAndRender(Uri(uri), db)
    res.config.contentType.mediaType match {
      case MediaTypes.`image/png` =>
        dir.mkdirs()
        val image = PngImage(res.data)
        val file = new File(dir, fname)
        scope(fileOut(file)) { out =>
          image.write(out)
        }

        val (w, h) = imageSize(file)
        val html = s"""<img src="$baseUri/$fname" alt="$fname" width="${w}px" height="${h}px"/>"""
        if (showQuery)
          s"${formatQuery(uri)}\n$html\n"
        else
          s"$html\n"

      case MediaTypes.`application/json` =>
        val data = s"```\n${prettyPrint(res.dataString)}\n```\n"
        if (showQuery) s"${formatQuery(uri)}\n$data" else data

      case _ =>
        val data = s"```\n${res.dataString}\n```\n"
        if (showQuery) s"${formatQuery(uri)}\n$data" else data
    }
  }

  def imageHtml(uri: String): String = {
    logger.info(s"creating image for: $uri")
    val fname = imageFileName(uri)
    val file = new File(dir, fname)
    if (!file.exists()) {
      val res = grapher.evalAndRender(Uri(uri), db)
      dir.mkdirs()
      val image = PngImage(res.data)
      scope(fileOut(file)) { out =>
        image.write(out)
      }
    }

    val (w, h) = imageSize(file)
    s"""<img src="$baseUri/$fname" alt="$fname" width="${w}px" height="${h}px"/>"""
  }

  private def imageSize(file: File): (Int, Int) = {
    val bytes = scope(fileIn(file))(byteArray)
    val image = PngImage(bytes).data
    image.getWidth -> image.getHeight
  }

  private def imageFileName(uri: String): String = {
    s"${Strings.zeroPad(Hash.sha1bytes(uri), 40).substring(0, 8)}.png"
  }

  def formatQuery(line: String): String = {
    val uri = URI.create(line)
    val params = Strings.parseQueryString(uri.getQuery)
    val pstr = params.toList.sortWith(_._1 < _._1).flatMap {
      case (k, vs) =>
        vs.map { v =>
          if (k == "q") formatQueryExpr(v) else s"$k=$v"
        }
    }
    s"<pre>\n${uri.getPath}?\n  ${pstr.mkString("\n  &")}\n</pre>\n"
  }

  private def mkLink(prg: List[Any], name: String): String = {
    try {
      val ctxt = interpreter.execute(prg)
      val candidates = interpreter.candidates(name, ctxt.stack)
      val vocab = vocabularies.find(v => v.words.contains(candidates.head)).get
      s"""<a href="$wordLinkBaseUri/${vocab.name}-${Utils.fileName(name)}">:$name</a>"""
    } catch {
      // For words inside a list, the execution is delayed. So the stack would depend on
      // when it gets executed. For these we link to the main index section showing the
      // alternatives for a name.
      case e: Exception =>
        s"""<a href="$wordLinkBaseUri/Stack-Language-Reference#${Utils.fileName(name)}">:$name</a>"""
    }
  }

  private def formatQueryExpr(q: String): String = {
    val parts = q.split(",").toList
    val buf = new StringBuilder
    buf.append("q=\n    ")
    parts.zipWithIndex.foreach {
      case (p, i) =>
        if (p.startsWith(":"))
          buf.append(mkLink(parts.take(i), p.substring(1))).append(',').append("\n    ")
        else
          buf.append(p).append(',')
    }
    val s = buf.toString
    s.substring(0, s.lastIndexOf(","))
  }

}
