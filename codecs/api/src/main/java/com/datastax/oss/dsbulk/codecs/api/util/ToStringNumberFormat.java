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
package com.datastax.oss.dsbulk.codecs.api.util;

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

  @Override
  public StringBuffer format(Object number, StringBuffer result, FieldPosition fieldPosition) {
    return result.append(number);
  }

  @Override
  public StringBuffer format(double number, StringBuffer result, FieldPosition fieldPosition) {
    return result.append(number);
  }

  @Override
  public StringBuffer format(long number, StringBuffer result, FieldPosition fieldPosition) {
    return result.append(number);
  }
}
