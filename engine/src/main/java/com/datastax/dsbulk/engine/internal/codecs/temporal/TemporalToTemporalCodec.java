/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;

public class TemporalToTemporalCodec<FROM extends TemporalAccessor, TO extends TemporalAccessor>
    extends ConvertingCodec<FROM, TO> {

  private final ZoneId timeZone;
  private final ZonedDateTime epoch;

  public TemporalToTemporalCodec(
      Class<FROM> javaType, TypeCodec<TO> targetCodec, ZoneId timeZone, ZonedDateTime epoch) {
    super(targetCodec, javaType);
    this.timeZone = timeZone;
    this.epoch = epoch;
  }

  @Override
  public FROM convertTo(TO value) {
    @SuppressWarnings("unchecked")
    Class<? extends FROM> targetClass = (Class<? extends FROM>) getJavaType().getRawType();
    return CodecUtils.convertTemporal(value, targetClass, timeZone, epoch.toLocalDate());
  }

  @Override
  public TO convertFrom(FROM value) {
    @SuppressWarnings("unchecked")
    Class<? extends TO> targetClass = (Class<? extends TO>) targetCodec.getJavaType().getRawType();
    return CodecUtils.convertTemporal(value, targetClass, timeZone, epoch.toLocalDate());
  }
}
