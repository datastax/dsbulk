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
package com.datastax.oss.dsbulk.runner.tests;

import java.net.URL;

public class JsonUtils {

  public static final URL JSON_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.json");
  public static final URL JSON_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.json");
  public static final URL JSON_RECORDS_UNIQUE_PART_1_DIR =
      ClassLoader.getSystemResource("part_1_json/");
  public static final URL JSON_RECORDS_UNIQUE_PART_2_DIR =
      ClassLoader.getSystemResource("part_2_json/");
  public static final URL JSON_RECORDS_UNIQUE_PART_1 =
      ClassLoader.getSystemResource("ip-by-country-unique-part-1.json");
  public static final URL JSON_RECORDS_UNIQUE_PART_2 =
      ClassLoader.getSystemResource("ip-by-country-unique-part-2.json");
  public static final URL JSON_RECORDS_CRLF =
      ClassLoader.getSystemResource("ip-by-country-crlf.json");
  public static final URL JSON_RECORDS_PARTIAL_BAD =
      ClassLoader.getSystemResource("ip-by-country-partial-bad.json");
  public static final URL JSON_RECORDS_SKIP =
      ClassLoader.getSystemResource("ip-by-country-skip-bad.json");
  public static final URL JSON_RECORDS_ERROR =
      ClassLoader.getSystemResource("ip-by-country-error.json");
  public static final URL JSON_RECORDS_WITH_SPACES =
      ClassLoader.getSystemResource("with-spaces.json");
  public static final URL JSON_RECORDS_WITH_COMMENTS =
      ClassLoader.getSystemResource("comments.json");
}
