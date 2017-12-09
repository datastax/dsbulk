/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;

public class NumberToNumberCodec<FROM extends Number, TO extends Number>
    extends ConvertingCodec<FROM, TO> {

  public NumberToNumberCodec(Class<FROM> javaType, TypeCodec<TO> targetCodec) {
    super(targetCodec, javaType);
  }

  @SuppressWarnings("unchecked")
  @Override
  public FROM convertTo(TO value) {
    return (FROM) CodecUtils.convertNumberExact(value, getJavaType().getRawType());
  }

  @SuppressWarnings("unchecked")
  @Override
  public TO convertFrom(FROM value) {
    return (TO) CodecUtils.convertNumberExact(value, targetCodec.getJavaType().getRawType());
  }
}
