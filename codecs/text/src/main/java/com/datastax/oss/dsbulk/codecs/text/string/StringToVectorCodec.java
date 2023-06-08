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
import com.datastax.oss.driver.internal.core.type.codec.CqlVectorCodec;
import java.util.List;

public class StringToVectorCodec<SubtypeT> extends StringConvertingCodec<CqlVector<SubtypeT>> {

  public StringToVectorCodec(CqlVectorCodec<SubtypeT> subcodec, List<String> nullStrings) {
    super(subcodec, nullStrings);
  }

  @Override
  public CqlVector<SubtypeT> externalToInternal(String s) {
    return this.internalCodec.parse(s);
  }

  @Override
  public String internalToExternal(CqlVector<SubtypeT> cqlVector) {
    return this.internalCodec.format(cqlVector);
  }
}
