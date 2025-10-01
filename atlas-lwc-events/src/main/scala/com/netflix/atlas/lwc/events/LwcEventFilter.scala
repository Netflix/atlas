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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.model.Query

/**
  * Filter for LWC events that can be used to support a post filter stage for certain
  * special dimensions.
  */
trait LwcEventFilter {

  /**
    * Returns the dimension used to project a value from the event.
    */
  def valueDimension: String

  /**
    * Split a query into two parts: an indexed query that can be evaluated efficiently,
    * and a post-filter query that is evaluated after matching events are retrieved.
    *
    * @param query
    *     The original query to split
    * @return
    *     A Queries object containing both the index query and post-filter query
    */
  def splitQuery(query: Query): LwcEventFilter.Queries

  /**
    * Check if an event matches the post-filter query.
    *
    * @param event
    *     The event to check
    * @param postFilterQuery
    *     The query to evaluate against the event
    * @return
    *     True if the event matches the query
    */
  def matches(event: LwcEvent, postFilterQuery: Query): Boolean
}

object LwcEventFilter {

  /**
    * Container for the split query components.
    *
    * @param indexQuery
    *     Query that can be indexed for efficient matching
    * @param postFilterQuery
    *     Query to be evaluated after index matching
    */
  case class Queries(indexQuery: Query, postFilterQuery: Query)

  /**
    * Returns the default implementation of LwcEventFilter.
    */
  def default: LwcEventFilter = new DefaultLwcEventFilter

  private class DefaultLwcEventFilter extends LwcEventFilter {

    override def valueDimension: String = "value"

    private def removeValueClause(query: Query): Query = {
      val q = query
        .rewrite {
          case kq: Query.KeyQuery if kq.k == "value" => Query.True
        }
        .asInstanceOf[Query]
      Query.simplify(q, ignore = true)
    }

    override def splitQuery(query: Query): Queries = {
      Queries(removeValueClause(query), Query.True)
    }

    override def matches(event: LwcEvent, postFilterQuery: Query): Boolean = true
  }
}
