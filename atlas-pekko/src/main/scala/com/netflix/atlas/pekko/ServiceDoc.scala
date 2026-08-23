/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.pekko

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.Link
import org.apache.pekko.http.scaladsl.model.headers.LinkParams
import org.apache.pekko.http.scaladsl.model.headers.LinkValue
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.PathMatcher
import org.apache.pekko.http.scaladsl.server.Route

/**
  * Agent facing document describing how to use the service. If a document is available, then
  * it is served at the well-known location and a `Link` header with a relation type of
  * `service-doc` is added to all responses so a client that has never seen the API has a
  * single place to look.
  *
  * The document and the header are handled together here so that the advertised location and
  * the location that is actually served cannot drift apart.
  *
  * @param title
  *     Human readable summary of what is at the other end of the link. This is what makes a
  *     client decide the fetch is worthwhile, so it should name the contents rather than just
  *     say "docs".
  * @param resource
  *     Classpath resource with the document to serve. Empty if there is no document, in which
  *     case nothing is served and no header will get added.
  * @param classLoader
  *     Class loader used to look up and serve the document. It is captured here rather than
  *     resolved when the route runs so that the loader used to check whether the resource is
  *     available is the same one used to serve it.
  */
case class ServiceDoc(title: String, resource: Option[String], classLoader: ClassLoader) {

  /** Header to add to responses pointing at the document, if there is one. */
  val linkHeader: Option[Link] = resource.map { _ =>
    val rel = LinkParams.rel("service-doc")
    val uri = Uri(ServiceDoc.wellKnownPath)
    val safeTitle = ServiceDoc.headerSafeTitle(title)
    val value =
      if (safeTitle.isEmpty) LinkValue(uri, rel)
      else LinkValue(uri, rel, LinkParams.title(safeTitle))
    Link(value)
  }

  /**
    * Route serving the document at the well-known location and the `llms.txt` alias. If there
    * is no document available, then the paths are not mapped and will result in the usual not
    * found response.
    */
  val routes: Route = resource.fold[Route](reject) { r =>
    (ServiceDoc.pathMatcher(ServiceDoc.wellKnownPath) |
      ServiceDoc.pathMatcher(ServiceDoc.aliasPath)) {
      getFromResource(r, ContentTypes.`text/plain(UTF-8)`, classLoader)
    }
  }
}

object ServiceDoc {

  /** Config block used to configure the document. */
  private val configPath = "atlas.pekko.service-doc"

  /**
    * Location for the document, using the well-known URI prefix from
    * [[https://www.rfc-editor.org/rfc/rfc8615.html RFC 8615]]. Note that `llms.txt` is not a
    * registered well-known URI, it is an emerging convention, so [[aliasPath]] is served as
    * well.
    */
  val wellKnownPath: String = "/.well-known/llms.txt"

  /** Alias for [[wellKnownPath]] following the emerging `llms.txt` convention. */
  val aliasPath: String = "/llms.txt"

  /** Class loader used to look up and serve the document resource. */
  def defaultClassLoader: ClassLoader = {
    val cl = Thread.currentThread().getContextClassLoader
    if (cl != null) cl else classOf[ServiceDoc].getClassLoader
  }

  private def pathMatcher(path: String) = {
    rawPathPrefix(PathMatcher(Uri.Path(path), ())) & pathEnd
  }

  /**
    * Make the title safe to render as the `title` parameter of the `Link` header. The value is
    * rendered into a quoted string without any escaping, so a raw quote or backslash would
    * produce a malformed header and a control character would cause the whole header to be
    * dropped while rendering the response.
    */
  private[pekko] def headerSafeTitle(title: String): String = {
    val builder = new StringBuilder(title.length)
    title.foreach {
      case c if Character.isISOControl(c) => // Drop control characters
      case c if c == '"' || c == '\\'     => builder.append('\\').append(c)
      case c                              => builder.append(c)
    }
    builder.toString()
  }

  /**
    * Create the service doc settings based on the `atlas.pekko.service-doc` config block. If
    * the configured resource is not present in the classpath, then it will not be served and
    * no link header will get used.
    */
  def apply(config: Config, classLoader: ClassLoader = defaultClassLoader): ServiceDoc = {
    val cfg = config.getConfig(configPath)
    val resource = cfg.getString("resource")
    val available = resource.nonEmpty && classLoader.getResource(resource) != null
    ServiceDoc(cfg.getString("title"), if (available) Some(resource) else None, classLoader)
  }
}
