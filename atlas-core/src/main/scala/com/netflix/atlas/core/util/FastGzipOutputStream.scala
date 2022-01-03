/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.core.util

import java.io.OutputStream
import java.util.zip.Deflater
import java.util.zip.GZIPOutputStream

/** Wrap GZIPOutputStream to set the best speed compression level. */
final class FastGzipOutputStream(out: OutputStream) extends GZIPOutputStream(out) {
  `def`.setLevel(Deflater.BEST_SPEED)
}
