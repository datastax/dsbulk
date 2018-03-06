/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import java.net.URL;

/** */
public class URLUtils {

  public static void setURLFactoryIfNeeded() {
    try {
      URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    } catch (Throwable t) {
      // URL.setURLStreamHandlerFactory throws an Error if it's been set more then once
      // Ignore that and keep going.
    }
  }
}
