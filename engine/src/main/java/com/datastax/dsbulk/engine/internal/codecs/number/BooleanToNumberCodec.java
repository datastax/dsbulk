/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class BooleanToNumberCodec<TO extends Number> extends ConvertingCodec<Boolean, TO> {

  private final List<TO> booleanNumbers;

  @SuppressWarnings("unchecked")
  public BooleanToNumberCodec(TypeCodec<TO> targetCodec, List<BigDecimal> booleanNumbers) {
    super(targetCodec, Boolean.class);
    this.booleanNumbers =
        booleanNumbers
            .stream()
            .map(n -> (TO) CodecUtils.convertNumber(n, targetCodec.getJavaType().getRawType()))
            .collect(Collectors.toList());
  }

  @Override
  public TO convertFrom(Boolean value) {
    if (value == null) {
      return null;
    }
    return booleanNumbers.get(value ? 0 : 1);
  }

  @Override
  public Boolean convertTo(TO value) {
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
}
