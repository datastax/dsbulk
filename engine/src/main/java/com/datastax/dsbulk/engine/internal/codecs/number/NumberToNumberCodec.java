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
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;

public class NumberToNumberCodec<FROM extends Number, TO extends Number>
    extends ConvertingCodec<FROM, TO> {

  public NumberToNumberCodec(Class<FROM> javaType, TypeCodec<TO> targetCodec) {
    super(targetCodec, javaType);
  }

  @SuppressWarnings("unchecked")
  @Override
  public FROM internalToExternal(TO value) {
    return CodecUtils.convertNumber(value, (Class<FROM>) getJavaType().getRawType());
  }

  @SuppressWarnings("unchecked")
  @Override
  public TO externalToInternal(FROM value) {
    return CodecUtils.convertNumber(value, (Class<TO>) internalCodec.getJavaType().getRawType());
  }
}
