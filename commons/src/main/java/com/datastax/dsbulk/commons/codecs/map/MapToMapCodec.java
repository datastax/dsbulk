/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.map;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapToMapCodec<EK, EV, IK, IV> extends ConvertingCodec<Map<EK, EV>, Map<IK, IV>> {
  private final ConvertingCodec<EK, IK> keyCodec;
  private final ConvertingCodec<EV, IV> valueCodec;

  public MapToMapCodec(
      Class<Map<EK, EV>> javaType,
      TypeCodec<Map<IK, IV>> targetCodec,
      ConvertingCodec<EK, IK> keyCodec,
      ConvertingCodec<EV, IV> valueCodec) {
    super(targetCodec, javaType);
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
  }

  @Override
  public Map<IK, IV> externalToInternal(Map<EK, EV> external) {
    if (external == null || external.isEmpty()) {
      return null;
    }

    Map<IK, IV> result = new LinkedHashMap<>();
    for (Map.Entry<EK, EV> entry : external.entrySet()) {
      result.put(
          keyCodec.externalToInternal(entry.getKey()),
          valueCodec.externalToInternal(entry.getValue()));
    }
    return result;
  }

  @Override
  public Map<EK, EV> internalToExternal(Map<IK, IV> internal) {
    if (internal == null || internal.isEmpty()) {
      return null;
    }

    Map<EK, EV> result = new LinkedHashMap<>();
    for (Map.Entry<IK, IV> entry : internal.entrySet()) {
      result.put(
          keyCodec.internalToExternal(entry.getKey()),
          valueCodec.internalToExternal(entry.getValue()));
    }
    return result;
  }
}
