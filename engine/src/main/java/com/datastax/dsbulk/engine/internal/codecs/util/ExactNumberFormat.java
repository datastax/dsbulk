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
 * A {@link NumberFormat} that converts floats and doubles to {@link java.math.BigDecimal} before
 * formatting.
 */
public class ExactNumberFormat extends NumberFormat {

  private final NumberFormat delegate;

  public ExactNumberFormat(NumberFormat delegate) {
    this.delegate = delegate;
  }

  @Override
  public Number parse(String source, ParsePosition parsePosition) {
    return delegate.parse(source, parsePosition);
  }

  @Override
  public StringBuffer format(Object number, StringBuffer result, FieldPosition fieldPosition) {
    // NumberFormat sometimes applies type narrowing / widening;
    // especially, for decimal values, it widens float -> double and
    // narrows BigDecimal -> double, then formats the resulting double.
    // This poses a problem for floats. The float -> double widening
    // may alter the original value; however, if we first convert float -> BigDecimal,
    // then let NumberFormat do the BigDecimal -> double narrowing, the result
    // *seems* exact for all floats.
    // To be on the safe side, let's convert all floating-point numbers to BigDecimals
    // before formatting.
    if (number instanceof Float || number instanceof Double) {
      try {
        number = CodecUtils.toBigDecimal((Number) number);
      } catch (NumberFormatException ignored) {
        // happens in rare cases, e.g. with Double.NaN
      }
    }
    return delegate.format(number, result, fieldPosition);
  }

  @Override
  public StringBuffer format(double number, StringBuffer result, FieldPosition fieldPosition) {
    return delegate.format(number, result, fieldPosition);
  }

  @Override
  public StringBuffer format(long number, StringBuffer result, FieldPosition fieldPosition) {
    return delegate.format(number, result, fieldPosition);
  }
}
