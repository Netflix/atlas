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
import java.net.URI
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams._
import com.netflix.atlas.core.util.Strings
import com.typesafe.scalalogging.StrictLogging
import spray.http.HttpMethods
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.Uri

import scala.util.Failure
import scala.util.Success

class GraphHelper(webApi: ActorRef, dir: File) extends StrictLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val timeout = akka.util.Timeout(1, TimeUnit.MINUTES)

  private val baseUri = "https://raw.githubusercontent.com/wiki/Netflix/atlas/gen-images"

  def image(uri: String, showQuery: Boolean = true): String = {
    logger.info(s"creating image for: $uri")
    val fname = imageFileName(uri)
    val req = HttpRequest(HttpMethods.GET, Uri(uri))
    akka.pattern.ask(webApi, req).onComplete {
      case Success(res: HttpResponse) =>
        dir.mkdirs()
        val image = PngImage(res.entity.data.toByteArray)
        scope(fileOut(new File(dir, fname))) { out => image.write(out) }
      case Success(v) => throw new IllegalStateException(s"unexpected response: $v")
      case Failure(t) => throw t
    }

    if (showQuery)
      s"${formatQuery(uri)}\n![$fname]($baseUri/$fname)\n"
    else
      s"![$fname]($baseUri/$fname)\n"
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
    s"```\n${uri.getPath}?\n  ${pstr.mkString("\n  &")}\n```\n"
  }

  private def formatQueryExpr(q: String): String = {
    val parts = q.split(",").toList
    val buf = new StringBuilder
    buf.append("q=\n    ")
    parts.foreach { p =>
      if (p.startsWith(":"))
        buf.append(p).append(',').append("\n    ")
      else
        buf.append(p).append(',')
    }
    val s = buf.toString
    s.substring(0, s.lastIndexOf(","))
  }

}
