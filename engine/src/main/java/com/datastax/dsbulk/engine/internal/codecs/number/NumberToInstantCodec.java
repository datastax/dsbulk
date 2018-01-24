/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class NumberToInstantCodec<FROM extends Number> extends ConvertingCodec<FROM, Instant> {

  private final TimeUnit numericTimestampUnit;
  private final Instant numericTimestampEpoch;

  public NumberToInstantCodec(
      Class<FROM> javaType, TimeUnit numericTimestampUnit, Instant numericTimestampEpoch) {
    super(InstantCodec.instance, javaType);
    this.numericTimestampUnit = numericTimestampUnit;
    this.numericTimestampEpoch = numericTimestampEpoch;
  }

  @Override
  public FROM convertTo(Instant value) {
    if (value == null) {
      return null;
    }
    long timestamp =
        CodecUtils.instantToTimestampSinceEpoch(value, numericTimestampUnit, numericTimestampEpoch);
    @SuppressWarnings("unchecked")
    FROM n = (FROM) CodecUtils.convertNumberExact(timestamp, getJavaType().getRawType());
    return n;
  }

  @Override
  public Instant convertFrom(FROM value) {
    if (value == null) {
      return null;
    }
    return CodecUtils.numberToInstant(value, numericTimestampUnit, numericTimestampEpoch);
  }
}
