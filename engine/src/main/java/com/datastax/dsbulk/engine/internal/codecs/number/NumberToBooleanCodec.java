/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class NumberToBooleanCodec<FROM extends Number> extends ConvertingCodec<FROM, Boolean> {

  private final List<FROM> booleanNumbers;

  @SuppressWarnings("unchecked")
  public NumberToBooleanCodec(Class<FROM> javaType, List<BigDecimal> booleanNumbers) {
    super(TypeCodec.cboolean(), javaType);
    this.booleanNumbers =
        booleanNumbers
            .stream()
            .map(n -> (FROM) CodecUtils.convertNumberExact(n, javaType))
            .collect(Collectors.toList());
  }

  @Override
  public Boolean convertFrom(FROM value) {
    if (value == null) {
      return null;
    }
    int i = booleanNumbers.indexOf(value);
    if (i == -1) {
      throw new InvalidTypeException(
          String.format(
              "Invalid boolean number %s, accepted values are %s (true) and %s (false)",
              value, booleanNumbers.get(0), booleanNumbers.get(1)));
    }
    return i == 0;
  }

  @Override
  public FROM convertTo(Boolean value) {
    if (value == null) {
      return null;
    }
    return booleanNumbers.get(value ? 0 : 1);
  }
}
