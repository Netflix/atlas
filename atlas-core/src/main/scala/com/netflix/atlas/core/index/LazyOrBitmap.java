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
package com.netflix.atlas.core.index;


import org.roaringbitmap.RoaringBitmap;

/**
 * Subclass to support access to protected methods for lazily computing OR across many
 * bitmaps. Similar to `FastAggregation.or`, but can be done manually to avoid having
 * to create an iterator.
 */
class LazyOrBitmap extends RoaringBitmap {

  public LazyOrBitmap() {
  }

  @Override
  public void naivelazyor(RoaringBitmap b) {
    super.naivelazyor(b);
  }

  @Override
  public void repairAfterLazy() {
    super.repairAfterLazy();
  }
}
