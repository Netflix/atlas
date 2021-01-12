/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.akka

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.RouteResult

import scala.concurrent.Promise

/**
  * Helper for porting some of the actor per request use-cases from spray over
  * to akka-http. Based on:
  *
  * https://markatta.com/codemonkey/blog/2016/08/03/actor-per-request-with-akka-http/
  */
case class ImperativeRequestContext(value: AnyRef, ctx: RequestContext) {

  val promise: Promise[RouteResult] = Promise()

  private implicit val ec = ctx.executionContext

  def complete(res: HttpResponse): Unit = {
    ctx.complete(res).onComplete(promise.complete)
  }

  def fail(t: Throwable): Unit = {
    ctx.fail(t).onComplete(promise.complete)
  }
}
