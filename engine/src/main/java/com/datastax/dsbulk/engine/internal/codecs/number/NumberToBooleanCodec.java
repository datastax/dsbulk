/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class NumberToBooleanCodec<EXTERNAL extends Number>
    extends ConvertingCodec<EXTERNAL, Boolean> {

  private final List<EXTERNAL> booleanNumbers;

  @SuppressWarnings("unchecked")
  public NumberToBooleanCodec(Class<EXTERNAL> javaType, List<BigDecimal> booleanNumbers) {
    super(TypeCodecs.BOOLEAN, javaType);
    this.booleanNumbers =
        booleanNumbers
            .stream()
            .map(n -> CodecUtils.convertNumber(n, javaType))
            .collect(Collectors.toList());
  }

  @Override
  public Boolean externalToInternal(EXTERNAL value) {
    if (value == null) {
      return null;
    }
    int i = booleanNumbers.indexOf(value);
    if (i == -1) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid boolean number %s, accepted values are %s (true) and %s (false)",
              value, booleanNumbers.get(0), booleanNumbers.get(1)));
    }
    return i == 0;
  }

  @Override
  public EXTERNAL internalToExternal(Boolean value) {
    if (value == null) {
      return null;
    }
    return booleanNumbers.get(value ? 0 : 1);
  }
}
