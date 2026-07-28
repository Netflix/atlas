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
    // The `caller` carried on these requests (and on DataRequest via its GraphConfig) is available
    // for attribution but is not consumed by this local database; a remote database can read it.
    case r: ListTagsRequest   => sender() ! TagListResponse(db.index.findTags(r.q))
    case r: ListKeysRequest   => sender() ! KeyListResponse(db.index.findKeys(r.q))
    case r: ListValuesRequest => sender() ! ValueListResponse(db.index.findValues(r.q))
    case req: DataRequest     => sender() ! executeDataRequest(req)
  }

  private def executeDataRequest(req: DataRequest): DataResponse = {
    val data = req.exprs.map(expr => expr -> db.execute(req.context, expr)).toMap
    DataResponse(req.context.step, data)
  }
}
