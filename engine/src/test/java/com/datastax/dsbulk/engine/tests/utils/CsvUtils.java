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

public class CsvUtils {

  public static final URL CSV_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.csv");
  public static final URL CSV_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.csv");
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
