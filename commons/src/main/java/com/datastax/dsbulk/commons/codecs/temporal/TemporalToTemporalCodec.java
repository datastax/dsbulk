/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.temporal;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;

public class TemporalToTemporalCodec<
        EXTERNAL extends TemporalAccessor, INTERNAL extends TemporalAccessor>
    extends ConvertingCodec<EXTERNAL, INTERNAL> {

  private final ZoneId timeZone;
  private final ZonedDateTime epoch;
  private final Class<EXTERNAL> rawJavaType;

  public TemporalToTemporalCodec(
      Class<EXTERNAL> javaType,
      TypeCodec<INTERNAL> targetCodec,
      ZoneId timeZone,
      ZonedDateTime epoch) {
    super(targetCodec, javaType);
    rawJavaType = javaType;
    this.timeZone = timeZone;
    this.epoch = epoch;
  }

  public Class<EXTERNAL> getRawJavaType() {
    return rawJavaType;
  }

  @Override
  public EXTERNAL internalToExternal(INTERNAL value) {
    @SuppressWarnings("unchecked")
    Class<? extends EXTERNAL> targetClass = (Class<? extends EXTERNAL>) getJavaType().getRawType();
    return CodecUtils.convertTemporal(value, targetClass, timeZone, epoch.toLocalDate());
  }

  @Override
  public INTERNAL externalToInternal(EXTERNAL value) {
    @SuppressWarnings("unchecked")
    Class<? extends INTERNAL> targetClass =
        (Class<? extends INTERNAL>) internalCodec.getJavaType().getRawType();
    return CodecUtils.convertTemporal(value, targetClass, timeZone, epoch.toLocalDate());
  }
}
