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
package com.netflix.atlas.pekko

import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.server.RequestContext
import org.apache.pekko.http.scaladsl.server.RouteResult

import scala.concurrent.ExecutionContext
import scala.concurrent.Promise

/**
  * Helper for porting some of the actor per request use-cases from spray over
  * to pekko-http. Based on:
  *
  * https://markatta.com/codemonkey/blog/2016/08/03/actor-per-request-with-pekko-http/
  */
case class ImperativeRequestContext(value: AnyRef, ctx: RequestContext) {

  val promise: Promise[RouteResult] = Promise()

  private implicit val ec: ExecutionContext = ctx.executionContext

  def complete(res: HttpResponse): Unit = {
    ctx.complete(res).onComplete(promise.complete)
  }

  def fail(t: Throwable): Unit = {
    ctx.fail(t).onComplete(promise.complete)
  }
}
