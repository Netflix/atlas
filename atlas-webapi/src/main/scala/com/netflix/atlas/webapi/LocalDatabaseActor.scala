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
package com.netflix.atlas.webapi

import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.ActorLogging
import com.netflix.atlas.core.db.Database

import scala.util.Failure

class LocalDatabaseActor(db: Database) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi.*
  import com.netflix.atlas.webapi.TagsApi.*

  def receive: Receive = {
    case v =>
      try innerReceive(v)
      catch {
        case t: Throwable => sender() ! Failure(t)
      }
  }

  private def innerReceive: Receive = {
    case ListTagsRequest(tq)   => sender() ! TagListResponse(db.index.findTags(tq))
    case ListKeysRequest(tq)   => sender() ! KeyListResponse(db.index.findKeys(tq))
    case ListValuesRequest(tq) => sender() ! ValueListResponse(db.index.findValues(tq))
    case req: DataRequest      => sender() ! executeDataRequest(req)
  }

  private def executeDataRequest(req: DataRequest): DataResponse = {
    val data = req.exprs.map(expr => expr -> db.execute(req.context, expr)).toMap
    DataResponse(req.context.step, data)
  }
}
