/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import java.net.URL;

/** */
public class URLUtils {

  public static void setURLFactoryIfNeeded() {
    try {
      URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    } catch (Exception e) {
      //URL.setURLStreamHandlerFactory throws an exception if it's been set more then once
      //Ignore that and keep going.
    }
  }
}
