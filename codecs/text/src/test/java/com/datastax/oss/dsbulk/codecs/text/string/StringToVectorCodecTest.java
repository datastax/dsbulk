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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.internal.core.type.codec.VectorCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

public class StringToVectorCodecTest {

  private final ArrayList<Float> values = Lists.newArrayList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f);
  private final CqlVector vector = CqlVector.newInstance(values);
  private final VectorCodec vectorCodec =
      new VectorCodec(new DefaultVectorType(DataTypes.FLOAT, 5), TypeCodecs.FLOAT);

  private final StringToVectorCodec dsbulkCodec =
      new StringToVectorCodec(vectorCodec, Lists.newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(dsbulkCodec)
        .convertsFromExternal(vectorCodec.format(vector)) // standard pattern
        .toInternal(vector)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(dsbulkCodec)
        .convertsFromInternal(vector)
        .toExternal(vectorCodec.format(vector))
        .convertsFromInternal(null)
        .toExternal("NULL");

    // We should encode
  }

  @Test
  void should_not_convert_from_invalid_internal() {
    assertThat(dsbulkCodec)
        .cannotConvertFromInternal("not a valid vector");
  }

  // To keep usage consistent with VectorCodec we confirm that we support encoding when too many elements are
  // available but not when too few are.  Note that it's actually VectorCodec that enforces this constraint so we
  // have to go through encode() rather than the internal/external methods.
  @Test
  void should_encode_too_many_but_not_too_few() {

    ArrayList<Float> tooMany = Lists.newArrayList(values);
    tooMany.add(6.6f);
    CqlVector<Float> tooManyVector = CqlVector.newInstance(tooMany);
    String tooManyString = dsbulkCodec.internalToExternal(tooManyVector);
    ArrayList<Float> tooFew = Lists.newArrayList(values);
    tooFew.remove(0);
    CqlVector<Float> tooFewVector = CqlVector.newInstance(tooFew);
    String tooFewString = dsbulkCodec.internalToExternal(tooFewVector);

    assertThat(dsbulkCodec.encode(tooManyString, ProtocolVersion.DEFAULT)).isNotNull();
    assertThatThrownBy(() -> dsbulkCodec.encode(tooFewString, ProtocolVersion.DEFAULT)).isInstanceOf(IllegalArgumentException.class);
  }
}
