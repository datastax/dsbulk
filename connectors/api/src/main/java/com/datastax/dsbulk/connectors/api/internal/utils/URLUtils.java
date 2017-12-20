/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api.internal.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

public class URLUtils {

  public static URL appendTimestamp(URL url) throws MalformedURLException {
    String original = url.toExternalForm();
    String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String newUrl = original + "-" + fileSuffix;
    return new URL(newUrl);
  }
}
