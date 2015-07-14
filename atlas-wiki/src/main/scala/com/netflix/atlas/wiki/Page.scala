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

trait Page {
  def name: String
  def path: Option[String]
  def content(graph: GraphHelper): String

  def file(baseDir: File): File = {
    val fname = s"$name.md"
    new File(baseDir, path.fold(fname)(v => s"$v/$fname"))
  }
}

trait SimplePage extends Page {
  def name: String = getClass.getSimpleName
  def path: Option[String] = None
}
