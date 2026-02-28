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
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.internal.core.type.codec.VectorCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.text.utils.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class StringToVectorCodec<SubtypeT extends Number>
    extends StringConvertingCodec<CqlVector<SubtypeT>> {

  private final ConvertingCodec<JsonNode, List<SubtypeT>> jsonCodec;
  private final ObjectMapper objectMapper;

  public StringToVectorCodec(
      VectorCodec<SubtypeT> targetCodec,
      ConvertingCodec<JsonNode, List<SubtypeT>> jsonCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public CqlVector<SubtypeT> externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    try {
      JsonNode node = objectMapper.readTree(StringUtils.ensureBrackets(s));
      List<SubtypeT> vals = jsonCodec.externalToInternal(node);
      return CqlVector.newInstance(vals);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Could not parse '%s' as Json", s), e);
    }
  }

  @Override
  public String internalToExternal(CqlVector<SubtypeT> cqlVector) {
    if (cqlVector == null) {
      return nullString();
    }
    try {
      List<SubtypeT> vals = cqlVector.stream().collect(Collectors.toList());
      JsonNode node = jsonCodec.internalToExternal(vals);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Could not format '%s' to Json", cqlVector), e);
    }
  }
}
