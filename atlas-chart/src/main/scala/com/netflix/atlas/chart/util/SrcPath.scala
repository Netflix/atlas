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
package com.netflix.atlas.chart.util

import java.io.File

/**
  * Utility for finding source path for the current sub project. This should be used sparingly.
  * If read-only is needed, then use getResource instead. The primary use-case is for tests
  * around images where there is an option to bless changes and update the golden files in the
  * source resources directory.
  */
object SrcPath {

  def forProject(name: String): String = {
    val cwd = new File(".").getCanonicalFile
    val subProject = new File(cwd, name)
    if (subProject.exists()) {
      // If not forking for tests or running in the ide, the working directory is typically
      // the root for the overall project.
      s"$subProject"
    } else if (cwd.getName == name) {
      // If the tests are forked, then the working directory is expected to be the directory
      // for the sub-project.
      "."
    } else {
      // Otherwise, some other case we haven't seen, force the user to figure it out
      throw new IllegalStateException(s"cannot determine source dir for $name, working dir: $cwd")
    }
  }
}
