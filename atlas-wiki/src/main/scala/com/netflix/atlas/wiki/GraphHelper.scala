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
package com.netflix.atlas.wiki

import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams._
import com.netflix.atlas.core.util.Strings
import com.typesafe.scalalogging.StrictLogging
import spray.http.HttpEntity
import spray.http.HttpMethods
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.MediaType
import spray.http.MediaTypes
import spray.http.Uri

import scala.concurrent.Await

class GraphHelper(webApi: ActorRef, dir: File, path: String) extends StrictLogging {

  import scala.concurrent.duration._

  implicit val timeout = akka.util.Timeout(1, TimeUnit.MINUTES)
  private val askRef = akka.pattern.ask(webApi)

  private val baseUri = s"https://raw.githubusercontent.com/wiki/Netflix/atlas/$path"

  private val wordLinkBaseUri = "https://github.com/Netflix/atlas/wiki"
  private val interpreter = Interpreter(StyleVocabulary.allWords)
  private val vocabularies = StyleVocabulary :: StyleVocabulary.dependencies

  override def toString: String = s"GraphHelper($dir, $path)"

  private def ct(res: HttpResponse): MediaType = {
    res.entity match {
      case e: HttpEntity.NonEmpty => e.contentType.mediaType
      case _  => throw new IllegalArgumentException("empty response entity")
    }
  }

  private def prettyPrint(json: String): String = {
    try {
      val mapper = new ObjectMapper()
      mapper.enable(SerializationFeature.INDENT_OUTPUT)
      mapper.writeValueAsString(mapper.readTree(json))
    } catch {
      case e: JsonParseException =>
        val factory = new JsonFactory()
        factory.enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
        factory.disable(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS)
        val mapper = new ObjectMapper(factory)
        mapper.enable(SerializationFeature.INDENT_OUTPUT)
        mapper.writeValueAsString(mapper.readTree(json))
    }
  }

  def image(uri: String, showQuery: Boolean = true): String = {
    logger.info(s"creating image for: $uri")
    val fname = imageFileName(uri)
    val req = HttpRequest(HttpMethods.GET, Uri(uri))
    val future = askRef.ask(req)
    Await.result(future, 1.minute) match {
      case res: HttpResponse =>
        ct(res) match {
          case MediaTypes.`image/png` =>
            dir.mkdirs()
            val image = PngImage(res.entity.data.toByteArray)
            val file = new File(dir, fname)
            scope(fileOut(file)) { out => image.write(out) }

            val (w, h) = imageSize(file)
            val html = s"""<img src="$baseUri/$fname" alt="$fname" width="${w}px" height="${h}px"/>"""
            if (showQuery)
              s"${formatQuery(uri)}\n$html\n"
            else
              s"$html\n"

          case MediaTypes.`application/json` =>
            val data = s"```\n${prettyPrint(res.entity.asString)}\n```\n"
            if (showQuery) s"${formatQuery(uri)}\n$data" else data

          case _ =>
            val data = s"```\n${res.entity.asString}\n```\n"
            if (showQuery) s"${formatQuery(uri)}\n$data" else data
        }
      case v => throw new IllegalStateException(s"unexpected response: $v")
    }
  }

  def imageHtml(uri: String): String = {
    logger.info(s"creating image for: $uri")
    val fname = imageFileName(uri)
    val file = new File(dir, fname)
    if (!file.exists()) {
      val req = HttpRequest(HttpMethods.GET, Uri(uri))
      val future = askRef.ask(req)
      Await.result(future, 1.minute) match {
        case res: HttpResponse =>
          dir.mkdirs()
          val image = PngImage(res.entity.data.toByteArray)
          scope(fileOut(file)) { out => image.write(out) }
        case v => throw new IllegalStateException(s"unexpected response: $v")
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
    s"${"%040x".format(Hash.sha1(uri)).substring(0, 8)}.png"
  }

  def formatQuery(line: String): String = {
    val uri = URI.create(line)
    val params = Strings.parseQueryString(uri.getQuery)
    val pstr = params.toList.sortWith(_._1 < _._1).flatMap { case (k, vs) =>
      vs.map { v => if (k == "q") formatQueryExpr(v) else s"$k=$v" }
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
    parts.zipWithIndex.foreach { case (p, i) =>
      if (p.startsWith(":"))
        buf.append(mkLink(parts.take(i), p.substring(1))).append(',').append("\n    ")
      else
        buf.append(p).append(',')
    }
    val s = buf.toString
    s.substring(0, s.lastIndexOf(","))
  }

}
