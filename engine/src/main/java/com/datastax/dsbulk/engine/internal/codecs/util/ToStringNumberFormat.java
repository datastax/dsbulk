/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;

/**
 * A {@link NumberFormat} that bypasses the formatting pattern and simply calls {@link
 * Number#toString()}.
 */
public class ToStringNumberFormat extends NumberFormat {

  private final NumberFormat delegate;

  public ToStringNumberFormat(NumberFormat delegate) {
    this.delegate = delegate;
  }

  @Override
  public Number parse(String source, ParsePosition parsePosition) {
    return delegate.parse(source, parsePosition);
  }

  public StringBuffer format(Object number, StringBuffer result, FieldPosition fieldPosition) {
    return result.append(number);
  }

  public StringBuffer format(double number, StringBuffer result, FieldPosition fieldPosition) {
    return result.append(number);
  }

  public StringBuffer format(long number, StringBuffer result, FieldPosition fieldPosition) {
    return result.append(number);
  }
}
