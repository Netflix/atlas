/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.atlas

import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.Dimension

/**
  * Helper types used in this package.
  */
package object cloudwatch {
  type Tags = Map[String, String]

  /**
    * Converts a list of cloudwatch dimensions into a tag map that can be used
    * for Atlas.
    */
  type Tagger = List[Dimension] => Tags

  /**
    * Converts a cloudwatch datapoint to a floating point value. The conversion is
    * based on the corresponding [[MetricDefinition]]. The full metadata is passed
    * in to allow access to other information that can be useful, such as the period
    * used for reporting the data into cloudwatch.
    */
  type Conversion = (MetricMetadata, Datapoint) => Double

  type AtlasDatapoint = com.netflix.atlas.core.model.Datapoint
  type MList = scala.collection.mutable.ListBuffer[AtlasDatapoint]
}
