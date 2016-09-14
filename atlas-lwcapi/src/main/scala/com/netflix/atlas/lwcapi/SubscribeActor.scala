/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import akka.actor.{Actor, ActorLogging}
import com.netflix.atlas.akka.DiagnosticMessage
import spray.http._

class SubscribeActor extends Actor with ActorLogging {
  import com.netflix.atlas.lwcapi.SubscribeApi._

  def receive = {
    case Spam(x) =>
      println(s"Sending $x chunks")
      sender() ! ChunkedResponseStart(HttpResponse(StatusCodes.OK))
      for(count <- 1 to x) {
        sender() ! MessageChunk("X" * 100 + "\n")
      }
      sender() ! ChunkedMessageEnd()
    case _ =>
      DiagnosticMessage.sendError(sender(), StatusCodes.BadRequest, "unknown payload")
  }
}
