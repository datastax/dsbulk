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
package com.datastax.oss.dsbulk.codecs.jdk.map;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
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
