/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.tests.utils;

import java.net.URL;

public class JsonUtils {

  public static final URL JSON_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.json");
  public static final URL JSON_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.json");
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
