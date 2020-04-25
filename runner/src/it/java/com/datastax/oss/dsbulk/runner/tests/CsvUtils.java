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

public class CsvUtils {

  public static final URL CSV_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.csv");
  public static final URL CSV_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.csv");
  public static final URL CSV_RECORDS_UNIQUE_PART_1_DIR =
      ClassLoader.getSystemResource("part_1_csv/");
  public static final URL CSV_RECORDS_UNIQUE_PART_2_DIR =
      ClassLoader.getSystemResource("part_2_csv/");
  public static final URL CSV_RECORDS_UNIQUE_PART_1 =
      ClassLoader.getSystemResource("ip-by-country-unique-part-1.csv");
  public static final URL CSV_RECORDS_UNIQUE_PART_2 =
      ClassLoader.getSystemResource("ip-by-country-unique-part-2.csv");
  public static final URL CSV_RECORDS_CRLF =
      ClassLoader.getSystemResource("ip-by-country-crlf.csv");
  public static final URL CSV_RECORDS_PARTIAL_BAD =
      ClassLoader.getSystemResource("ip-by-country-partial-bad.csv");
  public static final URL CSV_RECORDS_SKIP =
      ClassLoader.getSystemResource("ip-by-country-skip-bad.csv");
  public static final URL CSV_RECORDS_LONG =
      ClassLoader.getSystemResource("ip-by-country-long-column.csv");
  public static final URL CSV_RECORDS_ERROR =
      ClassLoader.getSystemResource("ip-by-country-error.csv");
  public static final URL CSV_RECORDS_WITH_SPACES =
      ClassLoader.getSystemResource("with-spaces.csv");
  public static final URL CSV_RECORDS_PARTIAL_BAD_LONG =
      ClassLoader.getSystemResource("ip-by-country-bad-long.csv");
}
