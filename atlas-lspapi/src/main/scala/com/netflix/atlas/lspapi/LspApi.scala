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
package com.netflix.atlas.lspapi

import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.lsp.AslLspServer
import com.netflix.atlas.lsp.Glossary
import com.netflix.atlas.lsp.UriLspServer
import com.netflix.atlas.pekko.WebApi
import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

class LspApi(config: Config) extends WebApi {

  private val vocabulary = new CustomVocabulary(config)

  private val glossary: Glossary = {
    val path = config.getString("atlas.lsp.glossary")
    if (path.isEmpty) Glossary.empty
    else Glossary.load(path)
  }

  def routes: Route = {
    pathPrefix("lsp" / "metrics") {
      extractMaterializer { implicit mat =>
        path("asl") {
          handleWebSocketMessages(LspWebSocketFlow(() => new AslLspServer(vocabulary, glossary)))
        } ~
        path("uri") {
          handleWebSocketMessages(LspWebSocketFlow(() => new UriLspServer(vocabulary, glossary)))
        }
      }
    }
  }
}
