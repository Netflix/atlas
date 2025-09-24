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
package com.netflix.atlas.json;

import java.util.function.Function;

/**
 * Simple test object to verify lambda issue with paranamer is fixed:
 *
 * https://github.com/FasterXML/jackson-module-paranamer/issues/13
 * https://github.com/paul-hammant/paranamer/issues/17
 *
 * <pre>
 * java.lang.ArrayIndexOutOfBoundsException: 55596
 *   at com.fasterxml.jackson.module.paranamer.shaded.BytecodeReadingParanamer$ClassReader.readUnsignedShort(BytecodeReadingParanamer.java:722)
 *   at com.fasterxml.jackson.module.paranamer.shaded.BytecodeReadingParanamer$ClassReader.accept(BytecodeReadingParanamer.java:571)
 *   at com.fasterxml.jackson.module.paranamer.shaded.BytecodeReadingParanamer$ClassReader.access$200(BytecodeReadingParanamer.java:338)
 *   at com.fasterxml.jackson.module.paranamer.shaded.BytecodeReadingParanamer.lookupParameterNames(BytecodeReadingParanamer.java:103)
 *   at com.fasterxml.jackson.module.paranamer.shaded.CachingParanamer.lookupParameterNames(CachingParanamer.java:79)
 * </pre>
 */
public class ObjWithLambda {

  private String foo;

  public void setFoo(String value) {
    final Function<String, String> check = v -> {
      if (v == null) throw new NullPointerException("value cannot be null");
      return v;
    };
    foo = check.apply(value);
  }

  public String getFoo() {
    return foo;
  }
}
