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
package com.datastax.oss.dsbulk.codecs.text;

import com.datastax.oss.dsbulk.codecs.CommonConversionContext;
import com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

public class TextConversionContext extends CommonConversionContext {

  public static final String OBJECT_MAPPER = "OBJECT_MAPPER";

  public TextConversionContext() {
    addAttribute(OBJECT_MAPPER, JsonCodecUtils.getObjectMapper());
  }

  public TextConversionContext setObjectMapper(@NonNull ObjectMapper objectMapper) {
    addAttribute(OBJECT_MAPPER, Objects.requireNonNull(objectMapper));
    return this;
  }
}
