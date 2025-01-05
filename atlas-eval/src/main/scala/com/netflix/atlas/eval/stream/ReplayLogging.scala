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
package com.netflix.atlas.eval.stream

import java.nio.charset.StandardCharsets

import org.apache.pekko.util.ByteString
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

/**
  * Helper class used to log events so they can be re-played in a controlled environment.
  * The user can control these using the normal logger settings. Exporting to a simple
  * text file with each message on a separate line is needed to replay.
  *
  * Since this level of logging is very verbose and likely will incur a high overhead, the
  * level is set to trace.
  */
private[stream] object ReplayLogging extends StrictLogging {

  def log(msg: ByteString): ByteString = {
    if (msg.nonEmpty) {
      logger.trace(msg.decodeString(StandardCharsets.UTF_8))
    }
    msg
  }

  def log(msg: String): String = {
    val trimmed = msg.trim
    if (trimmed.nonEmpty) {
      logger.trace(trimmed)
    }
    trimmed
  }

  def log(ds: DataSources): DataSources = {
    logger.whenTraceEnabled {
      val json = Json.encode(ds)
      logger.trace(s"info: datasources $json")
    }
    ds
  }
}
