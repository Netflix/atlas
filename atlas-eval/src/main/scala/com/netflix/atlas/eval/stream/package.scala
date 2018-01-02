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
package com.netflix.atlas.eval

import akka.NotUsed
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.json.JsonSupport

import scala.util.Try

package object stream {

  type Client = Flow[(HttpRequest, AccessLogger), (Try[HttpResponse], AccessLogger), NotUsed]
  type SimpleClient = Flow[HttpRequest, Try[HttpResponse], NotUsed]

  type SourcesAndGroups = (DataSources, EurekaSource.Groups)

  type DataSourceLogger = (DataSource, JsonSupport) => Unit

  type InstanceSource = Source[ByteString, NotUsed]
  type InstanceSources = List[InstanceSource]
  type InstanceSourceRef = SourceRef[ByteString, NotUsed]
}
