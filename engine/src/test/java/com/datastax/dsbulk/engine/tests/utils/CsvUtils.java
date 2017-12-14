/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.tests.utils;

import java.net.URL;

/** */
public class CsvUtils {

  public static final URL CSV_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.csv");
  public static final URL CSV_RECORDS_COMPLEX =
      ClassLoader.getSystemResource("ip-by-country-sample-complex.csv");
  public static final URL CSV_RECORDS_HEADER =
      ClassLoader.getSystemResource("ip-by-country-unique-header.csv");
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
