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
package com.datastax.oss.dsbulk.codecs.jdk.temporal;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
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
