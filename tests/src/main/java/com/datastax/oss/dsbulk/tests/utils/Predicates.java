/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.driver.shaded.guava.common.base.Functions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class Predicates {

  public static Predicate<String> jsonStringWithField(String fieldName, String fieldValue) {
    return jsonStringWithField(fieldName, fieldValue, Optional.empty());
  }

  public static Predicate<String> jsonStringWithField(
      String fieldName, String fieldValue, Optional<Function<String, String>> transform) {
    return arg -> {
      try {

        JsonNode argJson = new ObjectMapper().readTree(arg);
        if (!argJson.has(fieldName)) return false;
        return transform
            .orElse(Functions.identity())
            .apply(argJson.get(fieldName).toString())
            .equals(fieldValue);
      } catch (Exception e) {
        return false;
      }
    };
  }

  public static Predicate<JsonNode> jsonNodeWithField(String fieldName, String fieldValue) {
    return arg -> {
      try {

        return arg.get(fieldName).toString().equals(fieldValue);
      } catch (Exception e) {
        return false;
      }
    };
  }
}
