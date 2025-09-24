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
package com.netflix.atlas.eval

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TimeSeries

package object graph {

  type DataMap = Map[DataExpr, List[TimeSeries]]
  type StyleMap = Map[StyleExpr, List[TimeSeries]]
}
