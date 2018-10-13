/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.eval.model

import akka.util.ByteString
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport

/**
  * Helpers for working with messages coming back from the LWCAPI service.
  */
object LwcMessages {

  /**
    * Parse the message string into an internal model object based on the type.
    */
  def parse(msg: String): AnyRef = {
    val data = Json.decode[JsonNode](msg)
    data.get("type").asText() match {
      case "expression"   => Json.decode[LwcExpression](data)
      case "subscription" => Json.decode[LwcSubscription](data)
      case "datapoint"    => Json.decode[LwcDatapoint](data)
      case "diagnostic"   => Json.decode[LwcDiagnosticMessage](data)
      case "heartbeat"    => Json.decode[LwcHeartbeat](data)
      case _              => Json.decode[DiagnosticMessage](data)
    }
  }

  def toSSE(msg: JsonSupport): ByteString = {
    val prefix = msg match {
      case _: LwcSubscription      => subscribePrefix
      case _: LwcDatapoint         => metricDataPrefix
      case _: LwcDiagnosticMessage => diagnosticPrefix
      case _: LwcHeartbeat         => heartbeatPrefix
      case _                       => defaultPrefix
    }
    prefix ++ ByteString(msg.toJson) ++ suffix
  }

  val subscribePrefix = ByteString("info: subscribe ")
  val metricDataPrefix = ByteString("data: metric ")
  val diagnosticPrefix = ByteString("data: diagnostic ")
  val heartbeatPrefix = ByteString("data: heartbeat ")
  val defaultPrefix = ByteString("data: ")

  private val suffix = ByteString("\r\n\r\n")
}
